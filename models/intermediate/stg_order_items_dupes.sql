SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY created_at DESC) AS row_num
    FROM {{ ref('int_orderItems') }}
    QUALIFY COUNT(*) OVER (PARTITION BY order_id, product_id) > 1
) t
WHERE row_num =1