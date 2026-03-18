# =============================================================================
# Precious Metals RAG Pipeline — enrich_prices_dag.py
# 
# Purpose: BigQuery (Stats) -> FAISS -> Ollama (Reasoning) -> BigQuery
# =============================================================================

from __future__ import annotations
import os
import uuid
import logging
import re
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from langchain_ollama import OllamaLLM, OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# ── CONFIGURATION ────────────────────────────────────────────────────────────
BQ_DATASET = os.getenv("BQ_DATASET", "commodity_dataset")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_prices")
ENRICHED_TABLE = os.getenv("ENRICHED_TABLE", "enriched_analysis")
OLLAMA_BASE_URL = os.getenv("OLLAMA_HOST", "http://ollama:11434")
LOOKBACK_HOURS = 6

QUESTIONS = [
    ("Identify any volatility spikes (>0.3% change). Describe if these "
     "moves appear coordinated across all metals or isolated to one."),
    ("Compare market behavior between the first and second half. For Gold "
     "(XAU), is momentum accelerating or cooling down?"),
    ("Identify metals 'decoupled' from the trend. Based on this, what "
     "is the market sentiment (Risk-On/Risk-Off)?"),
    ("Analyze the Gold-to-Silver (XAU/XAG) ratio trajectory. What does "
     "this suggest about Silver's relative strength?"),
    ("Final Summary: Assign a trend (UP/DOWN/SIDEWAYS) to each metal, "
     "justifying it with price discovery from start to end.")
]

logger = logging.getLogger(__name__)

# ── ALERTING / CALLBACKS ─────────────────────────────────────────────────────

def on_failure_callback(context):
    """Triggered when a task fails."""
    ti = context['task_instance']
    err = context.get('exception')
    
    logging.error(
        f"🚨 TASK FAILURE: {ti.dag_id}.{ti.task_id} | "
        f"Logical Date: {context.get('logical_date')} | ERR: {err}"
    )

# ── DATA QUALITY GATE ────────────────────────────────────────────────────────

def check_for_new_data(**context):
    project_id = os.getenv("GCP_PROJECT_ID")
    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT COUNT(*) as row_count 
        FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}` 
        WHERE ingestion_timestamp >= 
              TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
    """
    results = bq_client.query(query).to_dataframe()
    return results.iloc[0]['row_count'] > 0

# ── UTILITIES ────────────────────────────────────────────────────────────────

class MetalRAGAnalyst:
    def __init__(self, model: str, embed_model: str):
        self.embeddings = OllamaEmbeddings(
            model=embed_model, 
            base_url=OLLAMA_BASE_URL
        )
        self.llm = OllamaLLM(
            model=model, 
            base_url=OLLAMA_BASE_URL, 
            temperature=0.1
        )

    def format_context(self, docs: List[Any]) -> str:
        return "\n\n".join([f"{d.page_content}" for d in docs])

    def create_vector_store(self, df: pd.DataFrame):
        texts = [
            (f"Time: {r['ingestion_timestamp']} | Metal: {r['metal']} | "
             f"Price: ${r['price_usd']:.2f} | "
             f"Change: {r['session_pct_change']}%") 
            for _, r in df.iterrows()
        ]
        return FAISS.from_texts(texts=texts, embedding=self.embeddings)

# ── MAIN TASK ────────────────────────────────────────────────────────────────

def run_rag_analysis(**context):
    project_id = os.getenv("GCP_PROJECT_ID")
    ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
    ollama_embed = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")

    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT metal, price_usd, ingestion_timestamp,
        ROUND(((price_usd - FIRST_VALUE(price_usd) 
            OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC)) / 
            NULLIF(FIRST_VALUE(price_usd) 
            OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC), 0)) * 100, 3) 
            as session_pct_change
        FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}`
        WHERE ingestion_timestamp >= 
              TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_HOURS} HOUR)
        ORDER BY ingestion_timestamp ASC
    """
    df = bq_client.query(query).to_dataframe()
    if df.empty:
        return

    analyst = MetalRAGAnalyst(ollama_model, ollama_embed)
    vectorstore = analyst.create_vector_store(df)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 15})

    prompt_str = (
        "You are a financial analyst. STRICT: Start with '1. ', no intro. "
        "Context: {context} Questions: {question} Answer:"
    )
    prompt = ChatPromptTemplate.from_template(prompt_str)
    
    chain = (
        {"context": retriever | analyst.format_context, 
         "question": RunnablePassthrough()} 
        | prompt | analyst.llm | StrOutputParser()
    )

    q_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(QUESTIONS)])
    mega_answer = chain.invoke(q_text)
    
    parts = re.split(r'\n?\d+\.\s+', mega_answer.strip())
    actual_answers = [p.strip() for p in parts if len(p.strip()) > 2]
    
    run_id = str(uuid.uuid4())
    run_at = datetime.utcnow().isoformat()
    
    results = []
    for idx, q in enumerate(QUESTIONS):
        ans = actual_answers[idx] if idx < len(actual_answers) else "N/A"
        results.append({
            "run_id": run_id,
            "run_at": run_at,
            "question": q,
            "answer": ans,
            "data_window_start": df["ingestion_timestamp"].min().isoformat(),
            "data_window_end": df["ingestion_timestamp"].max().isoformat()
        })

    if results:
        table_path = f"{project_id}.{BQ_DATASET}.{ENRICHED_TABLE}"
        bq_client.insert_rows_json(table_path, results)

# ── DAG DEFINITION ────────────────────────────────────────────────────────────

local_tz = pendulum.timezone("Europe/Warsaw")

default_args = {
    "owner": "analyst-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="enrich_precious_metals_with_rag",
    default_args=default_args,
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2026, 3, 18, tz=local_tz),
    catchup=False,
    max_active_runs=1,
) as dag:

    gate = ShortCircuitOperator(
        task_id="gate_check_new_data", 
        python_callable=check_for_new_data
    )
    
    rag = PythonOperator(
        task_id="run_local_rag_analysis", 
        python_callable=run_rag_analysis, 
        execution_timeout=timedelta(minutes=15)
    )

    gate >> rag