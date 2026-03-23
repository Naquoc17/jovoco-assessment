WITH customer_quarters AS (
    SELECT
        fs.customer_key,
        COUNT(DISTINCT (d.year * 10 + d.quarter)) AS num_quarters
    FROM gold.fact_sales fs
    JOIN gold.dim_date d ON fs.date_key = d.date_key
    WHERE fs.order_status != 'cancelled'
      AND fs.customer_key IS NOT NULL
    GROUP BY fs.customer_key
)
SELECT
    COUNT(*) FILTER (WHERE num_quarters > 1) AS multi_quarter,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE num_quarters > 1) / COUNT(*), 2)
        AS pct_multi_quarter
FROM customer_quarters;
