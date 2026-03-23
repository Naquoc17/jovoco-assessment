import psycopg2
import logging
from src.config import DB_CONFIG

logger = logging.getLogger(__name__)

def get_connection():
    """ Create connection to PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

def create_schemas(conn):
    """ Create 3 schema: stg, slv, gold """
    with conn.cursor() as cur:
        for schema in ["stg", "slv", "gold"]:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.info(f"Schema '{schema}' ready.")
    conn.commit()
