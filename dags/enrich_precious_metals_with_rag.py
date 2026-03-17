# =============================================================================
# Precious Metals RAG Pipeline — enrich_prices_dag.py
# 
# Purpose: BigQuery (Stats) -> FAISS -> Ollama (Reasoning) -> BigQuery
# Features: Data Quality Gate + Shift-Corrected Parsing + Failure Alerts.
# =============================================================================

from __future__ import annotations
import os
import uuid
import logging
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, List, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery

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

QUESTIONS = [
    "Identify any volatility spikes (>0.3% change) in the provided data. Describe if these moves appear coordinated across all metals or isolated to one.",
    "Compare the market behavior between the first half and second half of this session. Specifically for Gold (XAU), is the momentum accelerating or cooling down?",
    "Identify any metals currently 'decoupled' from the general market trend. Based on this, what is the overall market sentiment (Risk-On/Risk-Off)?",
    "Analyze the Gold-to-Silver (XAU/XAG) ratio trajectory. What does this suggest about Silver's relative strength in this session?",
    "Final Summary: Assign a trend (UP/DOWN/SIDEWAYS) to each metal, justifying it with the price discovery observed from start to end."
]

logger = logging.getLogger(__name__)

# ── ALERTING / CALLBACKS ─────────────────────────────────────────────────────

def on_failure_callback(context):
    """
    Function triggered when a task fails. 
    You can extend this to send Slack/Discord webhooks.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    err = context.get('exception')
    execution_date = context.get('execution_date')
    
    # Standard alert log - in production, replace with Slack/Discord webhook
    logging.error(f"""
    🚨 AIRFLOW TASK FAILURE:
    DAG:  {dag_id}
    TASk: {task_id}
    TIME: {execution_date}
    ERR:  {err}
    """)

# ── DATA QUALITY GATE ────────────────────────────────────────────────────────

def check_for_new_data(**context):
    project_id = os.getenv("GCP_PROJECT_ID")
    bq_client = bigquery.Client(project=project_id)
    query = f"SELECT COUNT(*) as row_count FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}` WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)"
    results = bq_client.query(query).to_dataframe()
    return results.iloc[0]['row_count'] > 0

# ── UTILITIES ────────────────────────────────────────────────────────────────

class MetalRAGAnalyst:
    def __init__(self, model: str, embed_model: str):
        self.embeddings = OllamaEmbeddings(model=embed_model, base_url=OLLAMA_BASE_URL)
        self.llm = OllamaLLM(model=model, base_url=OLLAMA_BASE_URL, temperature=0.1)

    def format_context(self, docs: List[Any]) -> str:
        return "\n\n".join([f"{d.page_content}" for d in docs])

    def create_vector_store(self, df: pd.DataFrame):
        texts = [f"Time: {r['ingestion_timestamp']} | Metal: {r['metal']} | Price: ${r['price_usd']:.2f} | Session Change: {r['session_pct_change']}%" for _, r in df.iterrows()]
        return FAISS.from_texts(texts=texts, embedding=self.embeddings)

# ── MAIN TASK ────────────────────────────────────────────────────────────────

def run_rag_analysis(**context):
    project_id = os.getenv("GCP_PROJECT_ID")
    ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
    ollama_embed = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")

    bq_client = bigquery.Client(project=project_id)
    query = f"""
        SELECT metal, price_usd, ingestion_timestamp,
        ROUND(((price_usd - FIRST_VALUE(price_usd) OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC)) / 
               NULLIF(FIRST_VALUE(price_usd) OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC), 0)) * 100, 3) as session_pct_change
        FROM `{project_id}.{BQ_DATASET}.{RAW_TABLE}`
        WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_HOURS} HOUR)
        ORDER BY ingestion_timestamp ASC
    """
    df = bq_client.query(query).to_dataframe()
    if df.empty: return

    analyst = MetalRAGAnalyst(ollama_model, ollama_embed)
    vectorstore = analyst.create_vector_store(df)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 15})

    prompt = ChatPromptTemplate.from_template("You are a financial analyst. STRICT: Start with '1. ', no intro. Context: {context} Questions: {question} Answer:")
    chain = ({"context": retriever | analyst.format_context, "question": RunnablePassthrough()} | prompt | analyst.llm | StrOutputParser())

    mega_answer = chain.invoke("\n".join([f"{i+1}. {q}" for i, q in enumerate(QUESTIONS)]))
    parts = re.split(r'\n?\d+\.\s+', mega_answer.strip())
    actual_answers = [p.strip() for p in parts if len(p.strip()) > 2]
    if len(actual_answers) > len(QUESTIONS): actual_answers = actual_answers[1:]

    run_id, run_at = str(uuid.uuid4()), datetime.utcnow().isoformat()
    results = [{"run_id": run_id, "run_at": run_at, "question": QUESTIONS[idx], "answer": (actual_answers[idx] if idx < len(actual_answers) else "N/A"), "data_window_start": df["ingestion_timestamp"].min().isoformat(), "data_window_end": df["ingestion_timestamp"].max().isoformat()} for idx in range(len(QUESTIONS))]

    if results:
        bq_client.insert_rows_json(f"{project_id}.{BQ_DATASET}.{ENRICHED_TABLE}", results)

# ── DAG DEFINITION ────────────────────────────────────────────────────────────

with DAG(
    dag_id="enrich_precious_metals_with_rag",
    default_args={
        "owner": "analyst-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback, # Alerts triggered here
    },
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 3, 17),
    catchup=False,
    max_active_runs=1,
) as dag:

    gate = ShortCircuitOperator(task_id="gate_check_new_data", python_callable=check_for_new_data)
    rag  = PythonOperator(task_id="run_local_rag_analysis", python_callable=run_rag_analysis, execution_timeout=timedelta(minutes=15))

    gate >> rag