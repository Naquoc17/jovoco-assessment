import os
import logging
from src.config import DATA_DIR

logger = logging.getLogger(__name__)

TABLES = {
    "customers.csv": {
        "table": "stg.customers",
        "columns": ["customer_id", "name", "city", "registration_date", "type"],
    },
    "orders.csv": {
        "table": "stg.orders",
        "columns": ["order_nr", "customer_name", "store", "order_date", "status"]
    },
    "order_items.csv": {
        "table": "stg.order_items",
        "columns": ["item", "order_nr", "product", "qty", "price"],
    },
    "products.csv": {
        "table": "stg.products",
        "columns": ["product_id", "title", "category", "cost"],
    },
    "stores.csv": {
        "table": "stg.stores",
        "columns": ["store_id", "title", "city", "region"],
    }
}

def run_bronze(conn):
    """ Load csv to schema stg """   

    with conn.cursor() as cur:
        for filename, meta in TABLES.items():
            table = meta["table"]
            columns = meta["columns"]
            filepath = os.path.join(DATA_DIR, filename)

            col_defs = ", ".join(f"{col} TEXT" for col in columns)
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
            cur.execute(f"CREATE TABLE {table} ({col_defs});")

            with open(filepath, "r", encoding="utf-8") as f:
                next(f)
                cur.copy_expert(
                    f"COPY {table} ({', '.join(columns)}) from STDIN with (FORMAT csv)",
                    f,
                )

            cur.execute(f"SELECT COUNT(*) FROM {table};")
            count = cur.fetchone()[0]
            logger.info(f" {table}: {count} rows loaded")

    conn.commit()
