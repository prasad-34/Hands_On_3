{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ ref('cleaned_orders') }}
SAMPLE (0.008)
