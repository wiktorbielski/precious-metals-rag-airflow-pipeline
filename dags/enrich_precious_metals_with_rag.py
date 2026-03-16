"""
Airflow DAG: enrich_precious_metals_with_rag

Performs local RAG-based analysis on recent precious metals prices using Ollama + FAISS.
Reads raw data from BigQuery → builds in-memory vector store → answers fixed questions → writes results back.

Requires:
- Ollama running at http://ollama:11434
- google-cloud-bigquery, langchain-ollama, faiss-cpu, pandas installed in Airflow image
- GCP service account key mounted at GCP_SERVICE_ACCOUNT_KEY_PATH
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os
import logging
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # ← correct import for Airflow ≥ 2.7 / 2.9+
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# ── Configuration ────────────────────────────────────────────────────────

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
if not GCP_PROJECT_ID:
    raise ValueError("Environment variable GCP_PROJECT_ID is required")

BQ_DATASET = os.getenv("BQ_DATASET", "commodity_dataset")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_prices")
ENRICHED_TABLE = os.getenv("ENRICHED_TABLE", "enriched_analysis")

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
OLLAMA_BASE_URL = "http://ollama:11434"  # docker network name

LOOKBACK_HOURS = 24
MAX_ROWS = 600          # safety limit

QUESTIONS = [
    "Rank the four metals (XAU, XAG, XPT, XPD) by current price (highest to lowest) and show the values.",
    "Which metal had the biggest % price change in the last 6 hours? Show the exact %.",
    "Compare gold (XAU) vs platinum (XPT) right now — which is more expensive and by how much %?",
    "Give a 1-2 sentence short-term trend summary for gold (XAU).",
]

logger = logging.getLogger(__name__)


def format_docs(docs: list[Any]) -> str:
    """Format retrieved documents into a readable string for LLM context."""
    lines = []
    for doc in docs:
        ts = doc.metadata.get("timestamp", "N/A")
        lines.append(f"{doc.page_content} (observed: {ts})")
    return "\n\n".join(lines)


def run_rag_analysis(**context: dict[str, Any]) -> None:
    """
    Main task logic: query recent prices → build RAG → answer questions → store results.
    """
    # ── GCP Auth ───────────────────────────────────────────────────────
    cred_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
    if not cred_path or not os.path.isfile(cred_path):
        raise RuntimeError(f"GCP credentials file not found: {cred_path}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # ── Fetch recent data ───────────────────────────────────────────────
    query = f"""
        SELECT metal, price_usd, ingestion_timestamp
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{RAW_TABLE}`
        WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_HOURS} HOUR)
        ORDER BY ingestion_timestamp DESC
        LIMIT {MAX_ROWS}
    """

    try:
        df: pd.DataFrame = client.query(query).to_dataframe()
        logger.info("Fetched %d rows from BigQuery", len(df))
    except GoogleAPIError as exc:
        logger.error("BigQuery query failed: %s", exc)
        raise

    if df.empty or len(df) < 4:
        logger.warning("Not enough data for meaningful RAG analysis (rows: %d)", len(df))
        return

    # ── Prepare documents ───────────────────────────────────────────────
    texts = [
        f"Metal: {row['metal']} | Price: {row['price_usd']} USD"
        for _, row in df.iterrows()
    ]

    # ── Build vector store ──────────────────────────────────────────────
    try:
        embeddings = OllamaEmbeddings(model=OLLAMA_EMBED_MODEL, base_url=OLLAMA_BASE_URL)
        vectorstore = FAISS.from_texts(texts=texts, embedding=embeddings)
        retriever = vectorstore.as_retriever(search_kwargs={"k": 12})
        logger.info("FAISS vector store built with %d documents", len(texts))
    except Exception as exc:
        logger.error("Failed to create embeddings / vector store: %s", exc)
        raise

    # ── LLM & Chain ─────────────────────────────────────────────────────
    llm = OllamaLLM(model=OLLAMA_MODEL, base_url=OLLAMA_BASE_URL, temperature=0.35)

    prompt = ChatPromptTemplate.from_template(
        """You are a professional precious metals market analyst.
Use **only** the provided recent price records.
Be concise, factual, include exact numbers when possible. Avoid speculation.

Recent prices:
{context}

Question: {question}

Answer:"""
    )

    chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )

    # ── Run questions ───────────────────────────────────────────────────
    results = []
    run_at_iso = datetime.utcnow().isoformat()

    for question in QUESTIONS:
        try:
            answer = chain.invoke(question)
            clean_answer = answer.strip()
            results.append({
                "question": question,
                "answer": clean_answer,
                "run_at": run_at_iso,
                "rows_considered": len(df),
            })
            logger.info("Q: %s\nA: %s\n%s", question, clean_answer, "-" * 60)
        except Exception as exc:
            logger.error("RAG failed for question '%s': %s", question, exc)

    # ── Save to BigQuery ────────────────────────────────────────────────
    if results:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{ENRICHED_TABLE}"
        try:
            errors = client.insert_rows_json(table_id, results)
            if errors:
                logger.error("BigQuery insert errors: %s", errors)
            else:
                logger.info("Saved %d analysis rows to %s", len(results), table_id)
        except GoogleAPIError as exc:
            logger.error("Failed to insert into BigQuery: %s", exc)
            raise

    # Cleanup
    del vectorstore


# ── DAG Definition ──────────────────────────────────────────────────────

default_args = {
    "owner": "analyst-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Optional: email_on_failure=True, email=['team@example.com']
}

with DAG(
    dag_id="enrich_precious_metals_with_rag",
    default_args=default_args,
    description="Local RAG analysis of recent precious metals prices (Ollama + FAISS)",
    schedule=timedelta(minutes=30),  # or "*/30 * * * *"
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=["rag", "ai", "metals", "finance"],
    max_active_runs=1,
) as dag:

    analyze = PythonOperator(
        task_id="run_local_rag_analysis",
        python_callable=run_rag_analysis,
        # provide_context=True  # ← not needed anymore in modern Airflow (kwargs always passed)
    )

    analyze