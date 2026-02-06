import csv
from datetime import datetime


def read_raw_events(file_path):
    events = []

    with open(file_path, mode="r", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            events.append({
                "user_id": row["user_id"],
                "action_type": row["action_type"],
                "action_value": int(row["action_value"]),
                "event_timestamp": datetime.fromisoformat(row["event_timestamp"])
            })

    return events
