import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# ── Configuration & GCP Auth ────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'commodity_prices')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_SERVICE_ACCOUNT_KEY_PATH = os.getenv('GCP_SERVICE_ACCOUNT_KEY_PATH')

# Set GCP credentials globally (same as your weather project)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCP_SERVICE_ACCOUNT_KEY_PATH

# BigQuery client & target table
client = bigquery.Client(project=GCP_PROJECT_ID)
table_id = f"{GCP_PROJECT_ID}.commodity_dataset.raw_prices"

# Batching settings (same logic as before)
BATCH_SIZE_THRESHOLD = 10

# Required/expected fields in each Kafka message
REQUIRED_FIELDS = [
    'metal', 'price_usd', 'api_timestamp', 'ingestion_timestamp'
]

# Kafka Consumer Setup (same reliable config as your weather consumer)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='commodity-prices-processing-group'
)

def validate_record(record: dict) -> bool:
    """
    Basic validation + schema handling.
    Skip if critical field (price) is missing; fill optional ones with None.
    """
    # Critical: no price → useless record
    if record.get('price_usd') is None:
        print(f"[SKIP] Missing price_usd for metal: {record.get('metal', 'UNKNOWN')}")
        return False

    # Warn and fill missing non-critical fields
    missing = [f for f in REQUIRED_FIELDS if f not in record]
    if missing:
        print(f"[WARN] Metal {record.get('metal', 'UNKNOWN')} missing fields: {missing}. Filling with None.")
        for field in missing:
            record[field] = None

    return True

def upload_batch(batch: list) -> bool:
    """Insert batch to BigQuery – same streaming insert logic as before."""
    if not batch:
        return True

    print(f"[UPLOAD] Sending {len(batch)} records to {table_id}...")
    try:
        errors = client.insert_rows_json(table_id, batch)
        if not errors:
            print(f"[OK] Batch of {len(batch)} records successfully loaded")
            return True

        print(f"[ERROR] BigQuery insert errors: {errors}")
        return False

    except Exception as e:
        print(f"[ERROR] BigQuery client exception: {type(e).__name__}: {e}")
        return False

# ── Main Consumer Loop ──────────────────────────────────────────────────────
print(f"[START] Commodity prices consumer listening on topic: '{TOPIC}'")
print(f"Target BigQuery table: {table_id}")
print(f"Batch size threshold: {BATCH_SIZE_THRESHOLD}\n")

data_batch = []

try:
    for message in consumer:
        try:
            record = message.value

            if not validate_record(record):
                continue

            # Add processing timestamp (UTC ISO format)
            record['processing_timestamp'] = datetime.now(timezone.utc).isoformat()

            data_batch.append(record)

            print(f"[RECEIVED] {record.get('metal')} | ${record.get('price_usd'):.2f} | "
                  f"Buffer: {len(data_batch)}/{BATCH_SIZE_THRESHOLD}")

            if len(data_batch) >= BATCH_SIZE_THRESHOLD:
                if upload_batch(data_batch):
                    data_batch = []  # clear only on success
                else:
                    print(f"[RETRY] Keeping {len(data_batch)} records in buffer for next attempt")

        except Exception as e:
            print(f"[ERROR] Message processing failed: {type(e).__name__}: {e}")
            continue

except KeyboardInterrupt:
    print("\n[STOP] Received Ctrl+C – shutting down gracefully...")

finally:
    # Final flush of any remaining records (even partial batch)
    if data_batch:
        print(f"[FINAL FLUSH] Saving {len(data_batch)} remaining records...")
        upload_batch(data_batch)

    consumer.close()
    print("[STOP] Consumer closed cleanly.")