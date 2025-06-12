select
ID,
ORDER_ID,
USER_ID,
PRODUCT_ID,
STATUS,
TRY_TO_TIMESTAMP(REPLACE(created_at, ' UTC', '')) AS created_at,
TRY_TO_TIMESTAMP(REPLACE(shipped_at, ' UTC', '')) AS shipped_at,
DELIVERED_AT,
RETURNED_AT,
SALE_PRICE 
from {{source('elook_commerce','order_items')}}

