"""
Airflow DAG: enrich_precious_metals_with_rag

Performs local RAG-based analysis on recent precious metals prices using Ollama + FAISS.
Reads raw data from BigQuery → builds in-memory vector store → answers fixed questions → writes results back.

Requires:
- Ollama running at http://ollama:11434 with nomic-embed-text and llama3.2:3b pulled
- google-cloud-bigquery, langchain-ollama, faiss-cpu, pandas installed in Airflow image
- GCP service account key mounted at /opt/airflow/gcp-key.json
"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timedelta
from typing import Any
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# ── Module-level constants (no env reads, no raises — safe at DAG parse time) ─
BQ_DATASET     = os.getenv("BQ_DATASET",    "commodity_dataset")
RAW_TABLE      = os.getenv("RAW_TABLE",     "raw_prices")
ENRICHED_TABLE = os.getenv("ENRICHED_TABLE","enriched_analysis")
OLLAMA_BASE_URL = "http://ollama:11434"
LOOKBACK_HOURS  = 6
MAX_ROWS        = 600   # 4 metals × 12 reads/hr × 6 hrs + headroom

QUESTIONS = [
    # ── Current snapshot (most recent reading) ────────────────────────────────
    "What is the latest recorded price for each of the four metals (XAU, XAG, XPT, XPD)? "
    "Show metal, price in USD, and the exact timestamp.",

    "Rank the four metals by their most recent price (highest to lowest) and show the exact USD values.",

    # ── Last 30 minutes ───────────────────────────────────────────────────────
    "Looking only at the last 30 minutes of data: what was the price change in USD and % for each metal? "
    "State direction (up/down/flat) for each.",

    "In the last 30 minutes, which metal moved the most in percentage terms? "
    "Which moved the least? Show exact % for all four.",

    "Were there any sudden price jumps or drops greater than 0.3% between consecutive 5-minute readings "
    "in the last 30 minutes? If yes, state the metal, timestamp, and exact % change.",

    # ── Full 6-hour window ────────────────────────────────────────────────────
    "For the entire recorded period, show the min price, max price, and price range (max - min) "
    "for each metal. Which metal had the widest range?",

    "Compare the opening price (earliest record) vs the closing price (latest record) for each metal "
    "over the full session. Which metal gained the most? Which lost the most? Show USD and % change.",

    "Rank the four metals by overall price volatility (price range) "
    "across the full session. Which was most stable?",

    "Divide the full session into two halves (first 3 hours vs last 3 hours). "
    "For gold (XAU), did the price trend differently between the two halves? Describe with numbers.",

    # ── Cross-metal relationships ─────────────────────────────────────────────
    "What is the current gold-to-silver ratio (XAU price / XAG price)? "
    "How did this ratio change from the start to the end of the recorded session?",

    "Compare gold (XAU) vs platinum (XPT) at the most recent price — "
    "which is more expensive and by how much in USD and %?",

    "Which two metals moved most in sync (similar % changes) over the full session? "
    "Which two diverged the most?",

    # ── Trend & momentum ─────────────────────────────────────────────────────
    "For each metal, is the overall trend across the full session UP, DOWN, or SIDEWAYS? "
    "Support each answer with the first price, last price, and % change.",

    "Looking at the last 30 minutes compared to the 30 minutes before that: "
    "is momentum for gold (XAU) accelerating, decelerating, or reversing? Use exact numbers.",
]

logger = logging.getLogger(__name__)


def format_docs(docs: list[Any]) -> str:
    """Format retrieved documents into a readable string for the LLM context."""
    lines = []
    for doc in docs:
        ts = doc.metadata.get("timestamp", "N/A")
        lines.append(f"{doc.page_content} (observed: {ts})")
    return "\n\n".join(lines)


def run_rag_analysis(**context: dict[str, Any]) -> None:
    """
    Main task:
      1. Validate env vars
      2. Fetch recent prices from BigQuery
      3. Build FAISS vector store via Ollama embeddings
      4. Run all QUESTIONS through the RAG chain
      5. Write results grouped by run_id back to BigQuery
    """

    # ── 1. Validate env vars (inside function — never at module/parse time) ───
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID is not set in environment")

    cred_path = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
    )
    if not cred_path or not os.path.isfile(cred_path):
        raise RuntimeError(f"GCP credentials file not found: {cred_path}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

    ollama_model       = os.getenv("OLLAMA_MODEL",           "llama3.2:3b")
    ollama_embed_model = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")

    # ── 2. Fetch recent data from BigQuery ────────────────────────────────────
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT
            metal,
            price_usd,
            ingestion_timestamp
        FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}`
        WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_HOURS} HOUR)
        ORDER BY ingestion_timestamp ASC
        LIMIT {MAX_ROWS}
    """

    try:
        df: pd.DataFrame = client.query(query).to_dataframe()
        logger.info("Fetched %d rows from BigQuery", len(df))
    except GoogleAPIError as exc:
        logger.error("BigQuery query failed: %s", exc)
        raise

    if df.empty or len(df) < 4:
        logger.warning("Not enough data for RAG analysis (rows: %d) — skipping", len(df))
        return

    # ── 3. Build FAISS vector store ───────────────────────────────────────────
    texts = [
        f"Metal: {row['metal']} | Price: {row['price_usd']:.4f} USD | Time: {row['ingestion_timestamp']}"
        for _, row in df.iterrows()
    ]

    try:
        embeddings  = OllamaEmbeddings(model=ollama_embed_model, base_url=OLLAMA_BASE_URL)
        vectorstore = FAISS.from_texts(texts=texts, embedding=embeddings)
        retriever   = vectorstore.as_retriever(search_kwargs={"k": 20})
        logger.info("FAISS vector store built with %d documents", len(texts))
    except Exception as exc:
        logger.error("Failed to build vector store: %s", exc)
        raise

    # ── 4. Build RAG chain ────────────────────────────────────────────────────
    llm = OllamaLLM(
        model=ollama_model,
        base_url=OLLAMA_BASE_URL,
        temperature=0.2,           # lower = more factual / deterministic
    )

    prompt = ChatPromptTemplate.from_template(
        """You are a professional precious metals market analyst.
Use ONLY the provided price records — do not use outside knowledge.
Be concise, factual, and always include exact numbers and timestamps where relevant.
If the data is insufficient to answer precisely, say so clearly.

Price records:
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

    # ── 5. Run all questions & collect results ────────────────────────────────
    run_id            = str(uuid.uuid4())
    run_at            = datetime.utcnow().isoformat()
    data_window_start = df["ingestion_timestamp"].min().isoformat()
    data_window_end   = df["ingestion_timestamp"].max().isoformat()

    logger.info(
        "Run %s | window %s → %s | %d rows | %d questions",
        run_id, data_window_start, data_window_end, len(df), len(QUESTIONS)
    )

    results = []
    for idx, question in enumerate(QUESTIONS):
        try:
            answer = chain.invoke(question)
            results.append({
                "run_id":             run_id,
                "run_at":             run_at,
                "question_index":     idx,
                "question":           question,
                "answer":             answer.strip(),
                "rows_considered":    len(df),
                "data_window_start":  data_window_start,
                "data_window_end":    data_window_end,
            })
            logger.info(
                "[%d/%d] Q: %s\nA: %s\n%s",
                idx + 1, len(QUESTIONS), question, answer.strip(), "-" * 60
            )
        except Exception as exc:
            logger.error("RAG failed for question %d ('%s'): %s", idx, question, exc)

    # ── 6. Write all results to BigQuery ──────────────────────────────────────
    if not results:
        logger.warning("No results to write — all questions failed")
        return

    table_id = f"{project_id}.{BQ_DATASET}.{ENRICHED_TABLE}"
    try:
        errors = client.insert_rows_json(table_id, results)
        if errors:
            logger.error("BigQuery insert errors: %s", errors)
        else:
            logger.info(
                "Saved %d rows to %s under run_id=%s",
                len(results), table_id, run_id
            )
    except GoogleAPIError as exc:
        logger.error("Failed to insert into BigQuery: %s", exc)
        raise

    # Cleanup
    del vectorstore


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":           "analyst-team",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="enrich_precious_metals_with_rag",
    default_args=default_args,
    description="Local RAG analysis of recent precious metals prices (Ollama + FAISS) — runs every 30 min",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=["rag", "ai", "metals", "finance"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_local_rag_analysis",
        python_callable=run_rag_analysis,
    )