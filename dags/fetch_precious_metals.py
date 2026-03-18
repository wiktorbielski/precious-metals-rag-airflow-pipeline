# =============================================================================
# Precious Metals RAG Pipeline — fetch_prices_dag.py
# 
# Purpose: Polls MetalpriceAPI every 5m and streams data into Kafka.
# Architecture: Airflow (Source) -> Kafka (Ingestion Layer)
# =============================================================================

import os
import json
import logging
import requests
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

# Load environment logic
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── CONFIGURATION ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
TOPIC = os.getenv('KAFKA_TOPIC', 'commodity_prices')
METALS = os.getenv('METALS', 'XAU,XAG,XPT,XPD')
API_KEY = os.getenv('METALPRICE_API_KEY')
BASE_URL = "https://api.metalpriceapi.com/v1/latest"

logger = logging.getLogger(__name__)

# ── TASK LOGIC ───────────────────────────────────────────────────────────────

def fetch_and_produce_metals(**context):
    """Fetches latest rates and produces 1 message per metal to Kafka."""
    if not API_KEY:
        raise ValueError("METALPRICE_API_KEY is missing from environment")

    # Initialize Producer with production-safe settings
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5,
        request_timeout_ms=30000,
    )

    try:
        logger.info(f"Requesting rates for: {METALS}")
        resp = requests.get(
            BASE_URL,
            params={
                'api_key': API_KEY,
                'base': 'USD',
                'currencies': METALS,
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get('success'):
            raise ValueError(f"API Error: {data}")

        # Process Timestamps
        raw_ts = data.get('timestamp')
        api_ts = (datetime.fromtimestamp(raw_ts).isoformat() 
                  if raw_ts else None)
        ingestion_ts = datetime.utcnow().isoformat()
        rates = data.get('rates', {})

        # Stream to Kafka
        for metal in METALS.split(','):
            metal = metal.strip()
            rate_key = f"USD{metal}"

            if rate_key not in rates:
                logger.warning(f"Metal {metal} not found in response.")
                continue

            payload = {
                'metal': metal,
                'price_usd': rates[rate_key],
                'api_timestamp': api_ts,
                'ingestion_timestamp': ingestion_ts,
                'airflow_run_id': context.get('run_id'),
            }

            producer.send(TOPIC, value=payload)
            logger.info(f"Streamed: {metal} @ ${payload['price_usd']:.2f}")

    except Exception as e:
        logger.error(f"Failed to fetch/stream metals: {str(e)}")
        raise
    finally:
        producer.flush()
        producer.close()

# ── DAG DEFINITION ───────────────────────────────────────────────────────────

default_args = {
    'owner': 'precious_metals_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

local_tz = pendulum.timezone("Europe/Warsaw")

with DAG(
    dag_id='fetch_precious_metals_prices',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    # Set the start_date to a fixed point in your local time
    start_date=pendulum.datetime(2026, 3, 18, tz=local_tz), 
    catchup=False,
    tags=['ingestion', 'kafka', 'metals'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_send_to_kafka',
        python_callable=fetch_and_produce_metals,
    )