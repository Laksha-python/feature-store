import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from storage.online_store import get_user_features


storage_dir = "storage"

print(get_user_features(storage_dir, "user_1"))
