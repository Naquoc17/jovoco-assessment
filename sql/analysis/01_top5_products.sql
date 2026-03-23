WITH quarter_bounds AS (
    SELECT
        DATE_TRUNC('quarter', MAX(d.full_date) - INTERVAL '3 months')::DATE AS quarter_start,
        (DATE_TRUNC('quarter', MAX(d.full_date))::DATE - INTERVAL '1 day')::DATE AS quarter_end
    FROM gold.fact_sales fs
    JOIN gold.dim_date d ON fs.date_key = d.date_key
),
ranked AS (
    SELECT
        s.region,
        p.product_title,
        SUM(fi.line_total) AS revenue,
        RANK() OVER (PARTITION BY s.region ORDER BY SUM(fi.line_total) DESC) AS rnk
    FROM gold.fact_sales fi
    JOIN gold.dim_date    d ON fi.date_key = d.date_key
    JOIN gold.dim_product p ON fi.product_key = p.product_key
    JOIN gold.dim_store   s ON fi.store_key = s.store_key
    CROSS JOIN quarter_bounds qb
    WHERE d.full_date BETWEEN qb.quarter_start AND qb.quarter_end
      AND fi.order_status != 'cancelled'
    GROUP BY s.region, p.product_title
)
SELECT region, product_title, ROUND(revenue, 2) AS revenue
FROM ranked
WHERE rnk <= 5
ORDER BY region, revenue DESC;
