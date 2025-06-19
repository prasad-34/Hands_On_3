-- depends_on: {{ ref('cleaned_orders') }}
-- depends_on: {{ ref('repeated_orders') }}

{{ config(
    materialized='incremental'
) }}

{% if execute %}
   
    {% set max_sql %}
        SELECT MAX(order_id) + 1 AS start_value
        FROM {{ this if is_incremental() else ref('cleaned_orders') }}
    {% endset %}

    {% set result = run_query(max_sql) %}
    {% set start_value = result.columns[0].values()[0] %}

    {% set seq_sql %}
        CREATE OR REPLACE SEQUENCE {{ this.schema }}.seq_incremental
        START WITH {{ start_value }}
        INCREMENT BY 1;
    {% endset %}

    {% do run_query(seq_sql) %}
{% endif %}


{% if not is_incremental() %}

SELECT
    order_id,
    user_id,
    status,
    gender,
    created_at,
    returned_at,
    shipped_at,
    delivered_at,
    num_of_items,
    '00000' AS duplicated_from

FROM {{ ref('cleaned_orders') }}
{% else %}

SELECT
    {{ this.schema }}.seq_incremental.NEXTVAL AS order_id,
    user_id,
    status,
    gender,
    CURRENT_TIMESTAMP AS created_at,

    DATEADD(
        second,
        DATEDIFF(second, created_at, TRY_TO_TIMESTAMP(returned_at)),
        CURRENT_TIMESTAMP
    ) AS returned_at,

    DATEADD(
        second,
        DATEDIFF(second, created_at, shipped_at),
        CURRENT_TIMESTAMP
    ) AS shipped_at,

    DATEADD(
        second,
        DATEDIFF(second, created_at, TRY_TO_TIMESTAMP(delivered_at)),
        CURRENT_TIMESTAMP
    ) AS delivered_at,

    num_of_items,
    CAST(order_id AS VARCHAR) AS duplicated_from

FROM {{ ref('repeated_orders') }}

{% endif %}

