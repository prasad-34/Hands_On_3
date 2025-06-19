{{ config(materialized='table') }}

SELECT DISTINCT
    o.status AS Status

FROM {{ ref('int_orders') }} AS o
   