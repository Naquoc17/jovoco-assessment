import logging

from src.utils import normalize_name, parse_date

logger = logging.getLogger(__name__)


def transform_stores(cur):
    cur.execute("DROP TABLE IF EXISTS slv.stores CASCADE;")
    cur.execute("""
        CREATE TABLE slv.stores (
            store_id INT PRIMARY KEY,
            title TEXT,
            city TEXT,
            region TEXT
        );
    """)

    cur.execute("""
        INSERT INTO slv.stores (store_id, title, city, region)
        SELECT DISTINCT
            CAST(store_id AS INT),
            INITCAP(title),
            city,
            COALESCE(NULLIF(region, ''), 'West')
        FROM stg.stores
        WHERE store_id IS NOT NULL
    AND store_id != '';
    """)


def transform_products(cur):
    cur.execute("DROP TABLE IF EXISTS slv.products CASCADE;")
    cur.execute("""
        CREATE TABLE slv.products (
            product_id INT PRIMARY KEY,
            title TEXT NOT NULL,
            category TEXT,
            cost NUMERIC(10,2)
        );
    """)

    cur.execute("""
        INSERT INTO slv.products (product_id, title, category, cost)
        SELECT
            CAST(product_id AS INT),
            title,
            category,
            CASE WHEN cost != '' THEN CAST(cost AS NUMERIC(10,2)) ELSE NULL END
        FROM stg.products
        WHERE product_id IS NOT NULL
        AND product_id != '';
    """)


def transform_customers(cur):
    cur.execute("DROP TABLE IF EXISTS slv.customers CASCADE;")
    cur.execute("""
        CREATE TABLE slv.customers (
            customer_id INT PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT,
            registration_date DATE,
            customer_type TEXT
        );
    """)

    cur.execute("""
        SELECT customer_id, name, city, registration_date, type
        FROM stg.customers;
    """)
    rows = cur.fetchall()

    for customer_id, name, city, reg_date, customer_type in rows:
        if not customer_id or not str(customer_id).strip():
            continue

        parsed_date = parse_date(reg_date) if reg_date else None

        cur.execute("""
            INSERT INTO slv.customers (
                customer_id, name, city, registration_date, customer_type
            )
            VALUES (%s, %s, %s, %s, %s);
        """, (
            int(customer_id),
            name.strip(),
            city.strip() if city and city.strip() else None,
            parsed_date,
            customer_type.strip() if customer_type and customer_type.strip() else None,
        ))


def transform_orders(cur):
    cur.execute("DROP TABLE IF EXISTS slv.orders CASCADE;")
    cur.execute("""
        CREATE TABLE slv.orders (
            order_id INT PRIMARY KEY,
            customer_id INT,
            store_id INT,
            order_date DATE,
            status TEXT
        );
    """)

    cur.execute("SELECT customer_id, name FROM slv.customers;")
    customer_lookup = {}
    for customer_id, name in cur.fetchall():
        customer_lookup[normalize_name(name)] = customer_id

    cur.execute("""
        SELECT order_nr, customer_name, store, order_date, status
        FROM stg.orders;
    """)
    rows = cur.fetchall()

    seen_orders = set()

    for order_nr, customer_name, store, order_date, status in rows:
        if not order_nr or not str(order_nr).strip():
            continue

        if order_nr in seen_orders:
            continue
        seen_orders.add(order_nr)

        normalized_customer = normalize_name(customer_name)
        customer_id = customer_lookup.get(normalized_customer)

        if customer_id is None:
            logger.warning("Order %s: customer not found for '%s'", order_nr, customer_name)

        parsed_date = parse_date(order_date) if order_date else None
        store_id = int(float(store)) if store and store.strip() else None
        clean_status = status.strip().lower() if status and status.strip() else None

        cur.execute("""
            INSERT INTO slv.orders (order_id, customer_id, store_id, order_date, status)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            int(order_nr),
            customer_id,
            store_id,
            parsed_date,
            clean_status,
        ))


def transform_order_items(cur):
    cur.execute("DROP TABLE IF EXISTS slv.order_items CASCADE;")
    cur.execute("""
        CREATE TABLE slv.order_items (
            item_id INT PRIMARY KEY,
            order_id INT,
            product_id INT,
            quantity INT,
            price NUMERIC(10,2)
        );
    """)

    cur.execute("SELECT product_id, title FROM slv.products;")
    product_lookup = {title.strip(): product_id for product_id, title in cur.fetchall()}

    cur.execute("""
        SELECT item, order_nr, product, qty, price
        FROM stg.order_items;
    """)
    rows = cur.fetchall()

    seen_items = set()

    for item_id, order_nr, product, qty, price in rows:
        if not item_id or not str(item_id).strip():
            continue

        if item_id in seen_items:
            continue
        seen_items.add(item_id)

        product_id = product_lookup.get(product.strip()) if product and product.strip() else None
        quantity = int(float(qty)) if qty and qty.strip() else 1
        price_value = float(price) if price and price.strip() else None

        cur.execute("""
            INSERT INTO slv.order_items (item_id, order_id, product_id, quantity, price)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            int(item_id),
            int(order_nr),
            product_id,
            quantity,
            price_value,
        ))


def run_silver(conn):
    with conn.cursor() as cur:
        transform_stores(cur)
        logger.info("  slv.stores done")

        transform_products(cur)
        logger.info("  slv.products done")

        transform_customers(cur)
        logger.info("  slv.customers done")

        transform_orders(cur)
        logger.info("  slv.orders done")

        transform_order_items(cur)
        logger.info("  slv.order_items done")

    conn.commit()
