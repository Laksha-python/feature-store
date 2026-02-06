import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

TOPIC = "user_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

USERS = [f"user_{i}" for i in range(1, 6)]
ACTIONS = ["view", "click", "purchase"]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def generate_event():
    action_type = random.choice(ACTIONS)

    return {
        "user_id": random.choice(USERS),
        "action_type": action_type,
        "action_value": random.randint(100, 2000) if action_type == "purchase" else 1,
        "event_timestamp": (
            datetime.now() - timedelta(days=random.randint(0, 30))
        ).isoformat()
    }


def main():
    producer = create_producer()

    for _ in range(50):
        event = generate_event()
        producer.send(TOPIC, event)
        print(f"Produced: {event}")
        time.sleep(0.1)

    producer.flush()
    producer.close()
    print("Producer finished.")


if __name__ == "__main__":
    main()
