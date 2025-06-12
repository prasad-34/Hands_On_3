-- models/cleaned/cleaned_order_items.sql

{{ config(materialized = 'table') }}

SELECT  id, order_id, user_id, product_id, status, created_at, shipped_at, delivered_at, returned_at, sale_price
 FROM {{ ref('stg_order_items_unique') }}
UNION ALL
SELECT  id, order_id, user_id, product_id, status, created_at, shipped_at, delivered_at, returned_at, sale_price
 FROM {{ ref('stg_order_items_dupes') }}
