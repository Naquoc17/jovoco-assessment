import logging
from src.database import get_connection, create_schemas
from src.bronze import run_bronze
from src.silver import run_silver
from src.gold import run_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    conn = get_connection()
    try:
        logger.info("Creating schemas")
        create_schemas(conn)

        logger.info("BRONZE: Loading raw CSV -> stg")
        run_bronze(conn)

        logger.info("SILVER: Transforming stg -> slv")
        run_silver(conn)

        logger.info("GOLD: Building star schema")
        run_gold(conn)

    except Exception as e:
        conn.rollback()
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
