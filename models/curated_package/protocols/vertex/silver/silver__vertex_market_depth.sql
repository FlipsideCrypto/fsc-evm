{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    full_refresh = false,
    unique_key = ['product_id','hour','price'],
    cluster_by = ['hour::DATE'],
    tags = 'curated'
) }}

WITH market_depth AS ({% for item in range(55) %}

    SELECT
        t.ticker_id, t.product_id, DATE_TRUNC('hour', TRY_TO_TIMESTAMP(t.timestamp)) AS HOUR, 'asks' AS orderbook_side, A.value [0] :: FLOAT AS price, A.value [1] :: FLOAT AS volume, SYSDATE() AS modified_timestamp
    FROM
        (
    SELECT
        response :ticker_id AS ticker_id, response :timestamp :: STRING AS TIMESTAMP, response :asks AS asks, response :bids AS bids, product_id
    FROM
        (
    SELECT
        PARSE_JSON(live.udf_api(CONCAT('https://gateway.' || '{{ vars.GLOBAL_PROJECT_NAME }}' || '-prod.vertexprotocol.com/v2/orderbook?ticker_id=', ticker_id, '&depth=1000000'))) :data AS response, product_id
    FROM
        (
    SELECT
        ROW_NUMBER() over (
    ORDER BY
        product_id) AS row_num, product_id, ticker_id
    FROM
        {{ ref('silver__vertex_dim_products') }}
    WHERE
        product_id > 0
    ORDER BY
        product_id)
    WHERE
        row_num = {{ item + 1 }})) t, LATERAL FLATTEN(input => t.asks) A
    UNION ALL
    SELECT
        t.ticker_id, t.product_id, DATE_TRUNC('hour', TRY_TO_TIMESTAMP(t.timestamp)) AS HOUR, 'bids' AS orderbook_side, A.value [0] :: FLOAT AS price, A.value [1] :: FLOAT AS volume, SYSDATE() AS modified_timestamp
    FROM
        (
    SELECT
        response :ticker_id AS ticker_id, response :timestamp :: STRING AS TIMESTAMP, response :asks AS asks, response :bids AS bids, product_id
    FROM
        (
    SELECT
        PARSE_JSON(live.udf_api(CONCAT('https://gateway.' || '{{ vars.GLOBAL_PROJECT_NAME }}' || '-prod.vertexprotocol.com/v2/orderbook?ticker_id=', ticker_id, '&depth=1000000'))) :data AS response, product_id
    FROM
        (
    SELECT
        ROW_NUMBER() over (
    ORDER BY
        product_id) AS row_num, product_id, ticker_id
    FROM
        {{ ref('silver__vertex_dim_products') }}
    WHERE
        product_id > 0
    ORDER BY
        product_id)
    WHERE
        row_num = {{ item + 1 }})) t, LATERAL FLATTEN(input => t.bids) A {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}),
    FINAL AS (
        SELECT
            ticker_id :: STRING AS ticker_id,
            product_id,
            HOUR,
            orderbook_side,
            price,
            ROUND(
                price,
                2
            ) AS round_price_0_01,
            ROUND(
                price,
                1
            ) AS round_price_0_1,
            ROUND(
                price,
                0
            ) AS round_price_1,
            ROUND(
                price,
                -1
            ) AS round_price_10,
            ROUND(
                price,
                -2
            ) AS round_price_100,
            volume,
            modified_timestamp
        FROM
            market_depth
    )
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['product_id','hour','price']
    ) }} AS vertex_market_depth_id,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
