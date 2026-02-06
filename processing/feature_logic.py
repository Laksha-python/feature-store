from datetime import timedelta
from collections import defaultdict


def compute_event_count_last_7d(events, reference_time):
    window_start = reference_time - timedelta(days=7)
    counts = defaultdict(int)

    for event in events:
        if event["event_timestamp"] >= window_start:
            counts[event["user_id"]] += 1

    return dict(counts)


def compute_purchase_count_last_30d(events, reference_time):
    window_start = reference_time - timedelta(days=30)
    counts = defaultdict(int)

    for event in events:
        if (
            event["action_type"] == "purchase"
            and event["event_timestamp"] >= window_start
        ):
            counts[event["user_id"]] += 1

    return dict(counts)


def compute_avg_purchase_value_last_30d(events, reference_time):
    window_start = reference_time - timedelta(days=30)
    totals = defaultdict(int)
    counts = defaultdict(int)

    for event in events:
        if (
            event["action_type"] == "purchase"
            and event["event_timestamp"] >= window_start
        ):
            user = event["user_id"]
            totals[user] += event["action_value"]
            counts[user] += 1

    averages = {}
    for user in totals:
        averages[user] = totals[user] / counts[user]

    return averages
