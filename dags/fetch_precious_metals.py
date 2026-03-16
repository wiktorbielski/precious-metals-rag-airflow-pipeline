from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator          # ← corrected import
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'commodity_prices')
BASE_URL = os.getenv('GOLD_API_BASE', 'https://www.gold-api.com/api')
METALS = os.getenv('METALS', 'XAU,XAG,XPT,XPD').split(',')

def fetch_and_produce_metals(**context):
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5,
        request_timeout_ms=30000,
    )

    for metal in METALS:
        try:
            url = f"{BASE_URL}/{metal}/USD"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if "price" not in data:
                raise ValueError(f"No price in response for {metal}")

            payload = {
                "metal": metal,
                "price_usd": data["price"],
                "api_timestamp": datetime.fromtimestamp(data.get("timestamp", 0)).isoformat() if data.get("timestamp") else None,
                "ingestion_timestamp": datetime.utcnow().isoformat(),
            }

            producer.send(TOPIC, value=payload)
            print(f"[Airflow] SENT → {metal} = {payload['price_usd']}")

        except Exception as e:
            print(f"[Airflow] ERROR for {metal}: {e}")
            raise  # Let Airflow retry

    producer.flush()

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='fetch_precious_metals_prices',
    default_args=default_args,
    description='Fetch gold/silver/platinum/palladium every ~60s',
    schedule='*/1 * * * *',          # every minute
    start_date=datetime(2026, 3, 4),
    catchup=False,
    tags=['streaming', 'finance'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_send_to_kafka',
        python_callable=fetch_and_produce_metals,
    )