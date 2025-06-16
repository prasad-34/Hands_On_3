
SELECT 
ORDER_ID,
USER_ID,
STATUS,
GENDER,
{{ time_travel('created_at',10,'day')}} AS CREATED_AT,
{{ time_travel('shipped_at',-5,'year')}} AS SHIPPED_AT,
RETURNED_AT,
DELIVERED_AT,
NUM_OF_ITEM,
FROM {{ ref ('Stg_orders') }}