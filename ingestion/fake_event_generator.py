import random
import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict


USERS = [f"user_{i}" for i in range(1, 6)]
ACTIONS = ["view", "click", "purchase"]


def generate_fake_events(num_events=50):
    events = []

    for _ in range(num_events):
        user_id = random.choice(USERS)
        action_type = random.choice(ACTIONS)
        action_value = random.randint(100, 2000) if action_type == "purchase" else 1

        event_time = datetime.now() - timedelta(
            days=random.randint(0, 30),
            minutes=random.randint(0, 59)
        )

        events.append({
            "user_id": user_id,
            "action_type": action_type,
            "action_value": action_value,
            "event_timestamp": event_time
        })

    return events


def compute_event_count_last_7d(events):
    window_start = datetime.now() - timedelta(days=7)
    counts = defaultdict(int)

    for event in events:
        if event["event_timestamp"] >= window_start:
            counts[event["user_id"]] += 1

    return counts


def compute_purchase_count_last_30d(events):
    window_start = datetime.now() - timedelta(days=30)
    counts = defaultdict(int)

    for event in events:
        if (
            event["action_type"] == "purchase"
            and event["event_timestamp"] >= window_start
        ):
            counts[event["user_id"]] += 1

    return counts


def compute_avg_purchase_value_last_30d(events):
    window_start = datetime.now() - timedelta(days=30)
    totals = defaultdict(int)
    counts = defaultdict(int)

    for event in events:
        if (
            event["action_type"] == "purchase"
            and event["event_timestamp"] >= window_start
        ):
            user_id = event["user_id"]
            totals[user_id] += event["action_value"]
            counts[user_id] += 1

    averages = {}
    for user_id in totals:
        averages[user_id] = totals[user_id] / counts[user_id]

    return averages


def store_online_features(event_counts_7d, purchase_counts_30d, avg_purchase_value_30d):
    today = datetime.now().strftime("%Y-%m-%d")
    base_path = f"storage/features/date={today}"
    os.makedirs(base_path, exist_ok=True)

    file_path = f"{base_path}/user_features.csv"
    computed_at = datetime.now().isoformat()

    all_users = set(
        event_counts_7d.keys()
        | purchase_counts_30d.keys()
        | avg_purchase_value_30d.keys()
    )

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "user_id",
            "event_count_last_7d",
            "purchase_count_last_30d",
            "avg_purchase_value_last_30d",
            "computed_at"
        ])

        for user_id in sorted(all_users):
            writer.writerow([
                user_id,
                event_counts_7d.get(user_id, 0),
                purchase_counts_30d.get(user_id, 0),
                round(avg_purchase_value_30d.get(user_id, 0), 2),
                computed_at
            ])


def write_offline_feature(feature_name, feature_values, feature_date):
    base_path = f"storage/offline_features/{feature_name}/date={feature_date}"
    os.makedirs(base_path, exist_ok=True)

    file_path = f"{base_path}/data.csv"

    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "feature_value", "feature_date"])

        for user_id, value in feature_values.items():
            writer.writerow([user_id, value, feature_date])


if __name__ == "__main__":
    print("Generating fake events...")
    events = generate_fake_events()

    print("Computing features...")
    event_counts_7d = compute_event_count_last_7d(events)
    purchase_counts_30d = compute_purchase_count_last_30d(events)
    avg_purchase_value_30d = compute_avg_purchase_value_last_30d(events)

    today = datetime.now().strftime("%Y-%m-%d")

    print("Writing OFFLINE feature history...")
    write_offline_feature(
        "user_event_count_last_7d",
        event_counts_7d,
        today
    )
    write_offline_feature(
        "user_purchase_count_last_30d",
        purchase_counts_30d,
        today
    )
    write_offline_feature(
        "user_avg_purchase_value_last_30d",
        avg_purchase_value_30d,
        today
    )

    print("Writing ONLINE feature snapshot...")
    store_online_features(
        event_counts_7d,
        purchase_counts_30d,
        avg_purchase_value_30d
    )

    print("Done. Offline history and online snapshot stored.")
