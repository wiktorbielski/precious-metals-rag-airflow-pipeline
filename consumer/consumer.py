import json
import os
import socket
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# ── Environment auto-detection ───────────────────────────────────────────────
# Inside Docker the hostname resolves; on Windows it won't → fall back to localhost
def _resolve_bootstrap_servers() -> str:
    env_val = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')
    if env_val:
        host = env_val.split(':')[0]
        try:
            socket.getaddrinfo(host, None)
            return env_val
        except socket.gaierror:
            print(f"[WARN] '{env_val}' not resolvable – falling back to localhost:9092")
            return 'localhost:9092'
    return 'localhost:9092'

# ── Configuration ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = _resolve_bootstrap_servers()
TOPIC             = os.getenv('KAFKA_TOPIC', 'commodity_prices')
GCP_PROJECT_ID    = os.getenv('GCP_PROJECT_ID')
KEY_PATH          = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')
BATCH_SIZE        = int(os.getenv('CONSUMER_BATCH_SIZE', '4'))
GROUP_ID          = os.getenv('KAFKA_CONSUMER_GROUP', 'commodity-prices-processing-group')

REQUIRED_FIELDS = ['metal', 'price_usd', 'api_timestamp', 'ingestion_timestamp']

# ── GCP Auth ─────────────────────────────────────────────────────────────────
if KEY_PATH:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH
else:
    print("[WARN] GCP_SERVICE_ACCOUNT_KEY_PATH not set – relying on ADC/gcloud login")

# ── BigQuery client ───────────────────────────────────────────────────────────
if not GCP_PROJECT_ID:
    raise EnvironmentError("GCP_PROJECT_ID is required but not set in .env")

bq_client = bigquery.Client(project=GCP_PROJECT_ID)
TABLE_ID  = f"{GCP_PROJECT_ID}.commodity_dataset.raw_prices"

# ── Kafka consumer ────────────────────────────────────────────────────────────
print(f"[INIT] Connecting to Kafka at {BOOTSTRAP_SERVERS}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    consumer_timeout_ms=-1,     # block forever – never time out
    request_timeout_ms=30000,
    session_timeout_ms=10000,
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def validate_record(record: dict) -> bool:
    """
    Returns False and skips records missing price_usd (critical).
    Fills any other missing required fields with None and continues.
    """
    if record.get('price_usd') is None:
        print(f"[SKIP] Missing price_usd for metal: {record.get('metal', 'UNKNOWN')}")
        return False

    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        print(f"[WARN] {record.get('metal', 'UNKNOWN')} missing optional fields {missing} – filling None")
        for field in missing:
            record[field] = None

    return True


def upload_batch(batch: list) -> bool:
    """Stream-insert a batch into BigQuery. Returns True on success."""
    if not batch:
        return True

    print(f"[UPLOAD] Inserting {len(batch)} records → {TABLE_ID}")
    try:
        errors = bq_client.insert_rows_json(TABLE_ID, batch)
        if not errors:
            print(f"[OK] {len(batch)} records committed to BigQuery")
            return True

        print(f"[ERROR] BigQuery reported insert errors: {errors}")
        return False

    except Exception as exc:
        print(f"[ERROR] BigQuery exception: {type(exc).__name__}: {exc}")
        return False


def flush(batch: list, label: str = "batch") -> list:
    """Upload batch and return empty list on success, same list on failure."""
    if upload_batch(batch):
        return []
    print(f"[RETRY] Keeping {len(batch)} records in buffer for next attempt")
    return batch


# ── Main loop ─────────────────────────────────────────────────────────────────
print(f"[START] Listening on topic '{TOPIC}'")
print(f"        Target table  : {TABLE_ID}")
print(f"        Batch size    : {BATCH_SIZE}")
print(f"        Consumer group: {GROUP_ID}\n")

data_batch: list = []

try:
    for message in consumer:
        try:
            record: dict = message.value

            if not validate_record(record):
                continue

            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()
            data_batch.append(record)

            print(
                f"[MSG] {record.get('metal'):>3} | "
                f"${record.get('price_usd'):>10.2f} | "
                f"buffer {len(data_batch)}/{BATCH_SIZE}"
            )

            if len(data_batch) >= BATCH_SIZE:
                data_batch = flush(data_batch)

        except Exception as exc:
            print(f"[ERROR] Failed to process message: {type(exc).__name__}: {exc}")
            continue

except KeyboardInterrupt:
    print("\n[STOP] Ctrl+C received – shutting down gracefully…")

finally:
    if data_batch:
        print(f"[FLUSH] Flushing {len(data_batch)} remaining records before exit…")
        flush(data_batch)

    consumer.close()
    print("[STOP] Consumer closed cleanly.")