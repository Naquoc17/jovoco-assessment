import logging

logger = logging.getLogger(__name__)

DIM_CUSTOMER_SQL = """
DROP TABLE IF EXISTS gold.dim_customer CASCADE;
CREATE TABLE gold.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT UNIQUE,
    full_name TEXT,
    city TEXT,
    customer_type TEXT,
    registration_date DATE
);

INSERT INTO gold.dim_customer (
    customer_id, full_name, city, customer_type, registration_date
)
SELECT
    customer_id,
    name,
    city,
    customer_type,
    registration_date
FROM slv.customers;
"""

DIM_PRODUCT_SQL = """
DROP TABLE IF EXISTS gold.dim_product CASCADE;
CREATE TABLE gold.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE,
    product_title TEXT,
    category TEXT,
    unit_cost NUMERIC(10,2)
);

INSERT INTO gold.dim_product (
    product_id, product_title, category, unit_cost
)
SELECT
    product_id,
    title,
    category,
    cost
FROM slv.products;
"""

DIM_STORE_SQL = """
DROP TABLE IF EXISTS gold.dim_store CASCADE;
CREATE TABLE gold.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id INT UNIQUE,
    store_title TEXT,
    city TEXT,
    region TEXT
);

INSERT INTO gold.dim_store (
    store_id, store_title, city, region
)
SELECT
    store_id,
    title,
    city,
    region
FROM slv.stores;
"""

DIM_DATE_SQL = """
DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name TEXT,
    day_of_week TEXT
);

INSERT INTO gold.dim_date (
    full_date, year, quarter, month, month_name, day_of_week
)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TRIM(TO_CHAR(d, 'Month')),
    TRIM(TO_CHAR(d, 'Day'))
FROM (
    SELECT
        COALESCE(MIN(order_date), CURRENT_DATE)::DATE AS start_date,
        COALESCE(MAX(order_date), CURRENT_DATE)::DATE AS end_date
    FROM slv.orders
    WHERE order_date IS NOT NULL
) bounds,
generate_series(bounds.start_date, bounds.end_date, '1 day') AS d;
"""

FACT_SALES_SQL = """
DROP TABLE IF EXISTS gold.fact_sales CASCADE;
CREATE TABLE gold.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id INT,
    item_id INT UNIQUE,
    customer_key INT REFERENCES gold.dim_customer(customer_key),
    product_key INT REFERENCES gold.dim_product(product_key),
    store_key INT REFERENCES gold.dim_store(store_key),
    date_key INT REFERENCES gold.dim_date(date_key),
    order_status TEXT,
    quantity INT,
    unit_price NUMERIC(10,2),
    line_total NUMERIC(10,2)
);

INSERT INTO gold.fact_sales (
    order_id,
    item_id,
    customer_key,
    product_key,
    store_key,
    date_key,
    order_status,
    quantity,
    unit_price,
    line_total
)
SELECT
    oi.order_id,
    oi.item_id,
    dc.customer_key,
    dp.product_key,
    ds.store_key,
    dd.date_key,
    o.status,
    oi.quantity,
    oi.price,
    oi.quantity * oi.price
FROM slv.order_items oi
LEFT JOIN slv.orders o
    ON oi.order_id = o.order_id
LEFT JOIN gold.dim_customer dc
    ON o.customer_id = dc.customer_id
LEFT JOIN gold.dim_product dp
    ON oi.product_id = dp.product_id
LEFT JOIN gold.dim_store ds
    ON o.store_id = ds.store_id
LEFT JOIN gold.dim_date dd
    ON o.order_date = dd.full_date;
"""

REVENUE_VIEW_SQL = """
CREATE OR REPLACE VIEW gold.v_revenue_over_time AS
SELECT
    d.year,
    d.quarter,
    d.month,
    DATE_TRUNC('month', d.full_date)::DATE AS month_start,
    SUM(fs.line_total) AS revenue
FROM gold.fact_sales fs
JOIN gold.dim_date d
    ON fs.date_key = d.date_key
WHERE fs.order_status != 'cancelled'
GROUP BY
    d.year,
    d.quarter,
    d.month,
    DATE_TRUNC('month', d.full_date)::DATE
ORDER BY month_start;
"""


def run_gold(conn):
    with conn.cursor() as cur:
        cur.execute(DIM_CUSTOMER_SQL)
        logger.info("  gold.dim_customer done")

        cur.execute(DIM_PRODUCT_SQL)
        logger.info("  gold.dim_product done")

        cur.execute(DIM_STORE_SQL)
        logger.info("  gold.dim_store done")

        cur.execute(DIM_DATE_SQL)
        logger.info("  gold.dim_date done")

        cur.execute(FACT_SALES_SQL)
        logger.info("  gold.fact_sales done")

        cur.execute(REVENUE_VIEW_SQL)
        logger.info("  gold.v_revenue_over_time done")

    conn.commit()
