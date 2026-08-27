import os

OPENSKY_URL = os.getenv(
    "OPENSKY_URL",
    "https://opensky-network.org/api/states/all"
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "flight-events"
)

POLL_INTERVAL_SECONDS = int(
    os.getenv(
        "POLL_INTERVAL_SECONDS",
        "60"
    )
)