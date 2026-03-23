import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "trade_analytics"),
    "user": os.getenv("DB_USER", "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_password"),
}

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
