import json
import time

import requests
from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    OPENSKY_URL,
    POLL_INTERVAL_SECONDS,
)


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def fetch_flights():
    response = requests.get(
        OPENSKY_URL,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def transform_flight(state, event_time):
    return {
        "event_time": event_time,
        "icao24": state[0],
        "callsign": state[1].strip() if state[1] else None,
        "origin_country": state[2],
        "time_position": state[3],
        "last_contact": state[4],
        "longitude": state[5],
        "latitude": state[6],
        "baro_altitude": state[7],
        "on_ground": state[8],
        "velocity": state[9],
        "true_track": state[10],
        "vertical_rate": state[11],
        "geo_altitude": state[13],
        "squawk": state[14],
    }


def main():
    producer = create_kafka_producer()

    print("Flight streaming producer started...")

    while True:
        try:
            data = fetch_flights()

            event_time = data.get("time")
            states = data.get("states") or []

            print(f"Received {len(states)} aircraft")

            for state in states:
                event = transform_flight(state, event_time)

                producer.send(
                    KAFKA_TOPIC,
                    value=event,
                )

            producer.flush()

            print(
                f"Published {len(states)} events "
                f"to Kafka topic '{KAFKA_TOPIC}'"
            )

        except requests.RequestException as error:
            print(f"OpenSky API error: {error}")

        except Exception as error:
            print(f"Producer error: {error}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()