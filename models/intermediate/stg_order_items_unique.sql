SELECT *
FROM {{ ref('int_orderItems') }}
QUALIFY COUNT(*) OVER (PARTITION BY order_id, product_id) =1