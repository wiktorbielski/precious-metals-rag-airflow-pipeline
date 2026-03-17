# =============================================================================
# Precious Metals RAG Pipeline — consumer.py
# 
# Purpose: Long-running process that drains Kafka and streams into BigQuery.
# Architecture: Kafka (Ingestion) -> Consumer (Process) -> BigQuery (Storage)
# =============================================================================

import json
import os
import socket
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Dict

from kafka import KafkaConsumer
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from dotenv import load_dotenv

# ── LOGGING SETUP ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

# ── NETWORK RESOLUTION ───────────────────────────────────────────────────────
def _resolve_bootstrap_servers() -> str:
    """Detects if we are in Docker or Host and returns appropriate Kafka URL."""
    env_val = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
    host = env_val.split(':')[0]
    try:
        socket.getaddrinfo(host, None)
        return env_val
    except socket.gaierror:
        logger.warning(f"Host '{host}' not resolvable. Falling back to localhost:9092")
        return 'localhost:9092'

# ── CONFIGURATION ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = _resolve_bootstrap_servers()
TOPIC             = os.getenv('KAFKA_TOPIC', 'commodity_prices')
GCP_PROJECT_ID    = os.getenv('GCP_PROJECT_ID')
BATCH_SIZE        = int(os.getenv('CONSUMER_BATCH_SIZE', '4'))
GROUP_ID          = os.getenv('KAFKA_CONSUMER_GROUP', 'metals-consumer-group')
TABLE_ID          = f"{GCP_PROJECT_ID}.commodity_dataset.raw_prices"

REQUIRED_FIELDS   = ['metal', 'price_usd', 'api_timestamp', 'ingestion_timestamp']

# ── CLIENT INITIALIZATION ─────────────────────────────────────────────────────
if not GCP_PROJECT_ID:
    raise EnvironmentError("GCP_PROJECT_ID must be set in .env")

# Path Logic: Fix for Windows vs Docker
key_path = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH', 'gcp-key.json')

# If running on Windows (nt) and the path is hardcoded for Linux/Docker, 
# we use the local version in the project root.
if os.name == 'nt' and key_path.startswith('/opt/airflow/'):
    logger.info("Windows detected: Translating Docker path to local path for gcp-key.json")
    key_path = 'gcp-key.json'

if not os.path.exists(key_path):
    logger.error(f"GCP Key not found at: {os.path.abspath(key_path)}")
    raise FileNotFoundError(f"Could not find {key_path}. Ensure it is in your project root.")

# Explicitly initialize using the path
bq_client = bigquery.Client.from_service_account_json(key_path, project=GCP_PROJECT_ID)
logger.info(f"BigQuery Client initialized with project: {GCP_PROJECT_ID}")

# ── CORE FUNCTIONS ───────────────────────────────────────────────────────────

def validate_and_clean(record: Dict) -> bool:
    """Ensures records meet schema requirements before BQ insertion."""
    if record.get('price_usd') is None:
        logger.warning(f"Skipping record: Missing price for {record.get('metal', 'UNKNOWN')}")
        return False
    
    # Fill missing optional metadata with None to maintain column alignment
    for field in REQUIRED_FIELDS:
        if field not in record:
            record[field] = None
            
    # Add a unique processing ID and timestamp for audit trails
    record['event_id'] = str(uuid.uuid4())
    record['processed_at'] = datetime.now(timezone.utc).isoformat()
    return True

def flush_to_bigquery(batch: List[Dict]) -> List[Dict]:
    """Attempts to insert batch. Returns empty list on success, batch on failure."""
    if not batch:
        return []

    logger.info(f"Streaming {len(batch)} records to BigQuery: {TABLE_ID}")
    try:
        errors = bq_client.insert_rows_json(TABLE_ID, batch)
        if not errors:
            logger.info("Successfully committed batch.")
            return []
        
        logger.error(f"BigQuery Insert Errors: {errors}")
        return batch # Keep in buffer for retry

    except (GoogleAPIError, Exception) as e:
        logger.error(f"Critical BigQuery failure: {str(e)}")
        return batch

# ── MAIN EXECUTION ───────────────────────────────────────────────────────────

def run_consumer():
    logger.info(f"Connecting to Kafka: {BOOTSTRAP_SERVERS} | Topic: {TOPIC}")
    
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=GROUP_ID,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000
        )
    except Exception as e:
        logger.error(f"Could not connect to Kafka: {e}")
        return

    data_buffer: List[Dict] = []

    try:
        for message in consumer:
            record = message.value
            
            if not validate_and_clean(record):
                continue

            data_buffer.append(record)
            # Safe logging for price formatting
            price = record.get('price_usd', 0.0)
            logger.info(f"Buffered: {record['metal']} | ${price:.2f}")

            if len(data_buffer) >= BATCH_SIZE:
                data_buffer = flush_to_bigquery(data_buffer)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        if data_buffer:
            logger.info("Flushing final records before exit...")
            flush_to_bigquery(data_buffer)
        consumer.close()
        logger.info("Consumer shutdown complete.")

if __name__ == "__main__":
    run_consumer()