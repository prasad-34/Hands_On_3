{{ config(materialized = 'table') }}
WITH item_counts AS (
    SELECT
        order_id,
        COUNT(*) AS num_of_items
    FROM {{ ref('cleaned_order_items') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.user_id,
    o.status,
    o.gender,
    {{ time_travel('created_at', 10, 'day') }} AS created_at,
    {{ time_travel('shipped_at', -5, 'year') }} AS shipped_at,
    o.returned_at,
    o.delivered_at,
    COALESCE(ic.num_of_items, 0) AS num_of_items  -- include derived column here

FROM {{ ref('int_orders') }} o
LEFT JOIN item_counts ic
    ON o.order_id = ic.order_id
