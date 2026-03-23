WITH customer_revenue AS (
    SELECT
        c.customer_key,
        c.full_name,
        SUM(fs.line_total) AS total_revenue
    FROM gold.fact_sales   fs
    JOIN gold.dim_customer c ON fs.customer_key = c.customer_key
    JOIN gold.dim_date     d ON fs.date_key = d.date_key
    WHERE d.full_date >= (
        SELECT MAX(full_date) - INTERVAL '12 months'
        FROM gold.dim_date dd
        JOIN gold.fact_sales ff ON dd.date_key = ff.date_key
    )
    AND fs.order_status != 'cancelled'
    GROUP BY c.customer_key, c.full_name
)
SELECT
    full_name,
    ROUND(total_revenue, 2) AS total_revenue,
    CASE NTILE(3) OVER (ORDER BY total_revenue DESC)
        WHEN 1 THEN 'High Value'
        WHEN 2 THEN 'Mid Value'
        WHEN 3 THEN 'Low Value'
    END AS segment
FROM customer_revenue
ORDER BY total_revenue DESC;
