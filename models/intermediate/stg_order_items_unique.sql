SELECT *
FROM {{ ref('Stg_Order_Items') }}
QUALIFY COUNT(*) OVER (PARTITION BY order_id, product_id) =1