WITH products_per_order AS (
    SELECT DISTINCT
        order_id,
        product_key
    FROM gold.fact_sales
    WHERE product_key IS NOT NULL
      AND order_status != 'cancelled'
)
SELECT
    p1.product_title AS product_a,
    p2.product_title AS product_b,
    COUNT(*) AS pair_count
FROM products_per_order f1
JOIN products_per_order f2
    ON f1.order_id = f2.order_id
   AND f1.product_key < f2.product_key
JOIN gold.dim_product p1 ON f1.product_key = p1.product_key
JOIN gold.dim_product p2 ON f2.product_key = p2.product_key
GROUP BY p1.product_title, p2.product_title
ORDER BY pair_count DESC, product_a, product_b
LIMIT 10;
