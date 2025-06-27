{{ config(materialized='table') }}
SELECT
   p.category as category,
   SUM(oi.SALE_PRICE) AS TotalRevenue
FROM {{ ref("cleaned_order_items") }} AS oi
JOIN {{ ref("int_orders") }} AS o
    ON oi.order_id = o.order_id
JOIN {{ ref("int_products") }} AS p
    ON oi.product_id = p.ID
WHERE o.status NOT IN ('Cancelled', 'Returned')
GROUP BY p.category
ORDER BY TotalRevenue DESC


