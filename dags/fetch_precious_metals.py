from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
TOPIC             = os.getenv('KAFKA_TOPIC', 'commodity_prices')
METALS            = os.getenv('METALS', 'XAU,XAG,XPT,XPD')
API_KEY           = os.getenv('METALPRICE_API_KEY')

# Single call fetches ALL metals at once — far more quota-efficient
# Response: { "success": true, "timestamp": 1234567890, "base": "USD",
#             "rates": { "USDXAU": 1985.23, "USDXAG": 26.10, ... } }
BASE_URL = "https://api.metalpriceapi.com/v1/latest"

def fetch_and_produce_metals(**context):
    if not API_KEY:
        raise ValueError("METALPRICE_API_KEY not set in environment")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5,
        request_timeout_ms=30000,
    )

    try:
        # One request for all metals
        resp = requests.get(
            BASE_URL,
            params={
                'api_key':   API_KEY,
                'base':      'USD',
                'currencies': METALS,   # e.g. "XAU,XAG,XPT,XPD"
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get('success'):
            raise ValueError(f"API returned error: {data}")

        rates      = data.get('rates', {})
        api_ts     = datetime.fromtimestamp(data['timestamp']).isoformat() if data.get('timestamp') else None
        ingest_ts  = datetime.utcnow().isoformat()

        for metal in METALS.split(','):
            metal = metal.strip()
            rate_key = f"USD{metal}"          # e.g. "USDXAU"

            if rate_key not in rates:
                print(f"[Airflow] WARN — {metal} not found in response, skipping")
                continue

            payload = {
                'metal':               metal,
                'price_usd':           rates[rate_key],
                'api_timestamp':       api_ts,
                'ingestion_timestamp': ingest_ts,
            }

            producer.send(TOPIC, value=payload)
            print(f"[Airflow] SENT → {metal} = ${payload['price_usd']:.2f}")

    except Exception as e:
        print(f"[Airflow] ERROR: {e}")
        raise  # preserves Airflow retry behaviour

    finally:
        producer.flush()
        producer.close()


default_args = {
    'owner':           'your-name',
    'depends_on_past': False,
    'retries':         3,
    'retry_delay':     timedelta(minutes=2),
}

with DAG(
    dag_id='fetch_precious_metals_prices',
    default_args=default_args,
    description='Fetch gold/silver/platinum/palladium every 5 minutes via MetalpriceAPI',
    schedule='*/5 * * * *',              # every 5 minutes
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['streaming', 'finance'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_send_to_kafka',
        python_callable=fetch_and_produce_metals,
    )