from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORE_DIR = BASE_DIR / "data_store"
DB_DIR = BASE_DIR / "db"
DB_URL = f"sqlite:///{DB_DIR / 'app.db'}"

REAL_DATA_DIR = DATA_STORE_DIR / "real"
SYNTHETIC_DATA_DIR = DATA_STORE_DIR / "synthetic"
SUBMISSIONS_DIR = DATA_STORE_DIR / "submissions"
RESULTS_DIR = DATA_STORE_DIR / "results"
LOGS_DIR = DATA_STORE_DIR / "logs"

SESSION_SECRET_KEY = "dev-secret-key-change-in-production"
EXECUTION_TIMEOUT_SECONDS = 60
