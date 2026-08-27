import json
import os
import time
from datetime import datetime, timezone
from io import BytesIO

from dotenv import load_dotenv
from kafka import KafkaConsumer
from databricks.sdk import WorkspaceClient


# Load environment variables
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_HOST or DATABRICKS_TOKEN is missing")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "flight-events"
KAFKA_GROUP_ID = "flight-databricks-consumer"

VOLUME_PATH = "/Volumes/flight_streaming/landing/raw_events"

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 30


# Databricks client
workspace = WorkspaceClient(
    host=DATABRICKS_HOST,
    token=DATABRICKS_TOKEN,
)


# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


def upload_batch(events):
    """Upload a batch of Kafka events as an NDJSON file to Databricks."""

    if not events:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    filename = f"flight_events_{timestamp}.json"
    remote_path = f"{VOLUME_PATH}/{filename}"

    ndjson = "\n".join(
        json.dumps(event, separators=(",", ":"))
        for event in events
    )

    data = BytesIO(ndjson.encode("utf-8"))

    print(f"Uploading {len(events)} events → {filename}")

    workspace.files.upload(
        remote_path,
        data,
        overwrite=False,
    )

    print(f"Uploaded successfully → {remote_path}")


def main():
    print("Flight Kafka Consumer started...")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Target: {VOLUME_PATH}")

    events = []
    batch_start = time.time()

    try:
        while True:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=BATCH_SIZE,
            )

            for _, messages in records.items():
                for message in messages:
                    events.append(message.value)

            elapsed = time.time() - batch_start

            if (
                len(events) >= BATCH_SIZE
                or (events and elapsed >= BATCH_TIMEOUT_SECONDS)
            ):
                upload_batch(events)

                consumer.commit()

                print(f"Committed {len(events)} Kafka events")

                events.clear()
                batch_start = time.time()

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()