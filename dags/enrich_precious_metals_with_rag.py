# =============================================================================
# Precious Metals RAG Pipeline — enrich_prices_dag.py
# 
# Purpose: BigQuery -> FAISS -> Ollama -> BigQuery
# Architecture: Enrichment layer performing automated market analysis via RAG.
# =============================================================================

from __future__ import annotations
import os
import uuid
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# ── CONFIGURATION ────────────────────────────────────────────────────────────
BQ_DATASET      = os.getenv("BQ_DATASET", "commodity_dataset")
RAW_TABLE       = os.getenv("RAW_TABLE", "raw_prices")
ENRICHED_TABLE  = os.getenv("ENRICHED_TABLE", "enriched_analysis")
OLLAMA_BASE_URL = os.getenv("OLLAMA_HOST", "http://ollama:11434")
LOOKBACK_HOURS  = 6
MAX_ROWS        = 600

# ── ANALYTICAL QUESTIONS ─────────────────────────────────────────────────────
# Grouped by analytical intent for easier maintenance
QUESTIONS = [
    # 1. Snapshot Analysis
    "What is the latest recorded price for each metal (XAU, XAG, XPT, XPD)? Show metal, price in USD, and timestamp.",
    "Rank metals by most recent price (highest to lowest).",
    
    # 2. Short-term Momentum (30m)
    "In the last 30 mins: what was the USD and % change for each metal? State direction (up/down/flat).",
    "Which metal moved the most/least in % terms in the last 30 mins?",
    "Identify any volatility spikes (>0.3% change) in the last 30 mins between consecutive readings.",
    
    # 3. Session Window (6h)
    "Show min, max, and price range for each metal over the full session. Identify the highest volatility.",
    "Compare 'Opening' (earliest) vs 'Closing' (latest) prices. Which metal gained/lost most?",
    "Divide the session into two halves. For XAU, compare the trend between the first and second half.",
    
    # 4. Market Ratios & Correlation
    "What is the current Gold-to-Silver (XAU/XAG) ratio? How did it change since the session start?",
    "Which two metals moved most in sync vs. which two diverged most during this session?",
    
    # 5. Summary & Sentiment
    "Determine the overall trend (UP/DOWN/SIDEWAYS) for each metal, providing start/end prices as proof."
]

logger = logging.getLogger(__name__)

# ── UTILITIES ────────────────────────────────────────────────────────────────

class MetalRAGAnalyst:
    """Helper class to encapsulate RAG logic and chain orchestration."""
    
    def __init__(self, model: str, embed_model: str):
        self.embeddings = OllamaEmbeddings(model=embed_model, base_url=OLLAMA_BASE_URL)
        self.llm = OllamaLLM(
            model=model, 
            base_url=OLLAMA_BASE_URL, 
            temperature=0.1  # Low temperature for factual consistency
        )

    def format_context(self, docs: List[Any]) -> str:
        return "\n\n".join([f"{d.page_content} (Observed: {d.metadata.get('ts')})" for d in docs])

    def create_vector_store(self, df: pd.DataFrame):
        texts = [
            f"Metal: {r['metal']} | Price: {r['price_usd']:.4f} USD" 
            for _, r in df.iterrows()
        ]
        metadatas = [{"ts": r['ingestion_timestamp'].isoformat()} for _, r in df.iterrows()]
        return FAISS.from_texts(texts=texts, embedding=self.embeddings, metadatas=metadatas)

# ── MAIN TASK ────────────────────────────────────────────────────────────────

def run_rag_analysis(**context):
    """Orchestrates the data fetch, RAG execution, and BigQuery write-back."""
    
    # 1. Environment Guardrails
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id: raise ValueError("GCP_PROJECT_ID not set")

    ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
    ollama_embed = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")

    # 2. Data Acquisition
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT metal, price_usd, ingestion_timestamp
        FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}`
        WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_HOURS} HOUR)
        ORDER BY ingestion_timestamp ASC LIMIT {MAX_ROWS}
    """
    
    df = bq_client.query(query).to_dataframe()
    if df.empty or len(df) < 4:
        logger.warning("Insufficient data for RAG session. Skipping.")
        return

    # 3. RAG Setup
    analyst = MetalRAGAnalyst(ollama_model, ollama_embed)
    vectorstore = analyst.create_vector_store(df)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 25})

    prompt = ChatPromptTemplate.from_template("""
        You are a financial analyst. Use ONLY the provided context. 
        Context: {context}
        Question: {question}
        Answer:""")

    chain = (
        {"context": retriever | analyst.format_context, "question": RunnablePassthrough()}
        | prompt | analyst.llm | StrOutputParser()
    )

    # 4. Analysis Execution
    run_id = str(uuid.uuid4())
    run_at = datetime.utcnow().isoformat()
    results = []

    logger.info(f"Starting RAG Analysis [RunID: {run_id}] with {len(df)} price points.")

    for idx, q in enumerate(QUESTIONS):
        try:
            answer = chain.invoke(q)
            results.append({
                "run_id": run_id,
                "run_at": run_at,
                "question": q,
                "answer": answer.strip(),
                "data_window_start": df["ingestion_timestamp"].min().isoformat(),
                "data_window_end": df["ingestion_timestamp"].max().isoformat()
            })
        except Exception as e:
            logger.error(f"Failed Q[{idx}]: {str(e)}")

    # 5. Persist Results
    if results:
        table_id = f"{project_id}.{BQ_DATASET}.{ENRICHED_TABLE}"
        errors = bq_client.insert_rows_json(table_id, results)
        if errors:
            logger.error(f"BQ Insert Errors: {errors}")
        else:
            logger.info(f"Successfully saved {len(results)} analysis records.")

# ── DAG DEFINITION ────────────────────────────────────────────────────────────

with DAG(
    dag_id="enrich_precious_metals_with_rag",
    default_args={
        "owner": "analyst-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Automated LLM Market Analysis via Ollama/FAISS",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=["rag", "ai", "enrichment"],
    max_active_runs=1, # Crucial for local LLMs to avoid resource contention
) as dag:

    PythonOperator(
        task_id="run_local_rag_analysis",
        python_callable=run_rag_analysis,
    )