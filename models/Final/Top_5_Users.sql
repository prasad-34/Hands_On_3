{{ config(materialized='table') }}
SELECT
    u.FIRST_NAME AS FirstName,
    u.LAST_NAME AS LastName,
    SUM(oi.SALE_PRICE) AS TotalAmountSpent
FROM {{ ref("cleaned_order_items") }} AS oi
JOIN {{ ref("int_orders") }} AS o
    ON oi.order_id = o.order_id
JOIN {{ ref("int_users") }} AS u
    ON oi.user_id = u.ID
WHERE o.status NOT IN ('Cancelled', 'Returned')
GROUP BY u.FIRST_NAME, u.LAST_NAME
ORDER BY TotalAmountSpent DESC
FETCH FIRST 5 ROWS ONLY

