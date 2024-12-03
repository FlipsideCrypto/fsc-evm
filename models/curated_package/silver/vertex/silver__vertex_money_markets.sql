
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['ticker_id','hour'],
    cluster_by = ['HOUR::DATE'],
    tags = 'curated'
) }}

{# Main query starts here #}
WITH apr AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://gateway.' || '{{ var("GLOBAL_PROD_DB_NAME") }}' || '-prod.vertexprotocol.com/v2/apr'
            )
        ):data AS response
),
flattened AS (
SELECT
    DATE_TRUNC('hour', SYSDATE()) AS HOUR,
    CONCAT(
        f.value:symbol::string,
        {% if var('GLOBAL_PROD_DB_NAME') == 'blast' %}
            '_USDB'
        {% else %}
            '_USDC'
        {% endif %}
    ) AS ticker_id,
    f.value:symbol::string AS symbol,
    f.value:product_id::string AS product_id,
    f.value:deposit_apr::float AS deposit_apr,
    f.value:borrow_apr::float AS borrow_apr,
    f.value:tvl::float AS tvl
FROM
    apr A,
    LATERAL FLATTEN(
        input => response
    ) AS f
)
SELECT
    HOUR,
    ticker_id,
    symbol,
    product_id,
    deposit_apr,
    borrow_apr,
    tvl,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ticker_id','hour']
    ) }} AS vertex_money_markets_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened
WHERE product_id not in ('121','125')  qualify(ROW_NUMBER() over(PARTITION BY ticker_id, HOUR
ORDER BY
    inserted_timestamp DESC )) = 1