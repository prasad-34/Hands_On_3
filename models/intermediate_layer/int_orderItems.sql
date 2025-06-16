select
ID,
ORDER_ID,
USER_ID,
PRODUCT_ID,
STATUS,
{{ time_travel('created_at',10,'day')}} AS created_at,
{{ time_travel('shipped_at',-2,'month')}}AS shipped_at,
DELIVERED_AT,
RETURNED_AT,
SALE_PRICE 
from {{ref('Stg_Order_Items')}}

