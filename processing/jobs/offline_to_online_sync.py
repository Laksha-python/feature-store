import os
import shutil
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OFFLINE_DIR = os.path.join(BASE_DIR, "storage", "offline_features")
ONLINE_DIR = os.path.join(BASE_DIR, "storage", "online_store")

os.makedirs(ONLINE_DIR, exist_ok=True)


def sync_latest_features():
    today = datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(OFFLINE_DIR):
        print("Offline directory not found.")
        return

    for feature_name in os.listdir(OFFLINE_DIR):
        feature_path = os.path.join(OFFLINE_DIR, feature_name)

        if not os.path.isdir(feature_path):
            continue

        source_file = os.path.join(feature_path, f"{today}.json")

        if os.path.exists(source_file):
            target_file = os.path.join(ONLINE_DIR, f"{feature_name}.json")
            shutil.copyfile(source_file, target_file)
            print(f"Synced {feature_name}")
        else:
            print(f"Skipping {feature_name} (no file for today)")


if __name__ == "__main__":
    sync_latest_features()
    print("Offline → Online sync completed.")
