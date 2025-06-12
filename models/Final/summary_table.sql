{{ config(materialized='table') }}

SELECT DISTINCT
    o.status AS Status

FROM {{ ref('Stg_orders') }} AS o
   