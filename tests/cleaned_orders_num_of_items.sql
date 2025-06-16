-- tests/cleaned_orders_num_of_items.sql

WITH expected_counts AS (
    SELECT
        order_id,
        COUNT(*) AS expected_num_of_items
    FROM {{ ref('cleaned_order_items') }}
    GROUP BY order_id
),

actual_counts AS (
    SELECT
        order_id,
        num_of_items
    FROM {{ ref('cleaned_orders') }}
)

SELECT
    a.order_id,
    a.num_of_items,
    e.expected_num_of_items
FROM actual_counts a
JOIN expected_counts e
    ON a.order_id = e.order_id
WHERE a.num_of_items != e.expected_num_of_items
