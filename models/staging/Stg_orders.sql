-- models/staging/stg_orders.sql

SELECT 
ORDER_ID,
USER_ID,
STATUS,
GENDER,
TRY_TO_TIMESTAMP(REPLACE(created_at,' UTC','')) AS CREATED_AT,
TRY_TO_TIMESTAMP(REPLACE(shipped_at, ' UTC', '')) AS shipped_at,
RETURNED_AT,
DELIVERED_AT,
NUM_OF_ITEM,
FROM {{ source('elook_commerce', 'orders') }}

