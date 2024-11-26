{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['ticker_id','hour'],
    cluster_by = ['HOUR::DATE'],
    tags = 'curated'
) }}

{# Main query starts here #}
WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://gateway.' || '{{ var("GLOBAL_PROD_DB_NAME") }}' || '-prod.vertexprotocol.com/v2/contracts'
            )
        ) :data AS response
),
market_stats AS (
    SELECT
        DATE_TRUNC('hour', SYSDATE()) AS HOUR,
        f.value :base_currency :: STRING AS base_currency,
        f.value :base_volume :: FLOAT AS base_volume,
        f.value :contract_price :: FLOAT AS contract_price,
        f.value :contract_price_currency :: STRING AS contract_price_currency,
        f.value :funding_rate :: FLOAT AS funding_rate,
        f.value :index_price :: FLOAT AS index_price,
        f.value :last_price :: FLOAT AS last_price,
        f.value :mark_price :: FLOAT AS mark_price,
        TRY_TO_TIMESTAMP(
            f.value :next_funding_rate_timestamp :: STRING
        ) AS next_funding_rate_timestamp,
        f.value :open_interest :: FLOAT AS open_interest,
        f.value :open_interest_usd :: FLOAT AS open_interest_usd,
        f.value :price_change_percent_24h :: FLOAT AS price_change_percent_24h,
        f.value :product_type :: STRING AS product_type,
        f.value :quote_currency :: STRING AS quote_currency,
        f.value :quote_volume :: FLOAT AS quote_volume,
        f.key AS ticker_id,
        SYSDATE() AS inserted_timestamp
    FROM
        api_pull A,
        LATERAL FLATTEN(
            input => response
        ) AS f
),
trade_snapshot AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS HOUR,
        CONCAT(
            symbol,
            '_USDC'
        ) AS ticker_id,
        symbol,
        product_id,
        COUNT(DISTINCT(tx_hash)) AS distinct_sequencer_batches,
        COUNT(DISTINCT(trader)) AS distinct_trader_count,
        COUNT(DISTINCT(subaccount)) AS distinct_subaccount_count,
        COUNT(DISTINCT(digest)) AS trade_count,
        SUM(amount_usd) AS amount_usd,
        SUM(fee_amount) AS fee_amount,
        SUM(base_delta_amount) AS base_delta_amount,
        SUM(quote_delta_amount) AS quote_delta_amount,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ ref('silver__vertex_perps') }}
        p
    WHERE
        block_timestamp > '2024-11-01 00:00:00.000' --start of api pulls
    GROUP BY
        1,
        2,
        3,
        4
),
products AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        product_id,
        product_type,
        ticker_id,
        symbol,
        name,
        health_group,
        health_group_symbol,
        taker_fee,
        maker_fee,
        modified_timestamp,
        fact_event_logs_id,
        vertex_products_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__vertex_dim_products') }}
),
FINAL AS (
    SELECT
        s.hour,
        s.ticker_id,
        p.symbol,
        p.product_id,
        t.distinct_sequencer_batches,
        t.distinct_trader_count,
        t.distinct_subaccount_count,
        t.trade_count,
        t.amount_usd,
        t.fee_amount,
        t.base_delta_amount,
        t.quote_delta_amount,
        s.base_volume AS base_volume_24h,
        s.quote_volume AS quote_volume_24h,
        s.contract_price,
        s.contract_price_currency,
        s.funding_rate,
        s.index_price,
        s.last_price,
        s.mark_price,
        s.next_funding_rate_timestamp,
        s.open_interest,
        s.open_interest_usd,
        s.price_change_percent_24h,
        s.product_type,
        s.quote_currency,
        s.quote_volume,
        t.modified_timestamp,
    FROM
        market_stats s
        LEFT JOIN trade_snapshot t
        ON t.ticker_id = s.ticker_id
        AND s.hour = t.hour
        LEFT JOIN products p
        ON s.ticker_id = p.ticker_id

{% if is_incremental() %}
UNION ALL
SELECT
    s.hour,
    s.ticker_id,
    p.symbol,
    p.product_id,
    t.distinct_sequencer_batches,
    t.distinct_trader_count,
    t.distinct_subaccount_count,
    t.trade_count,
    t.amount_usd,
    t.fee_amount,
    t.base_delta_amount,
    t.quote_delta_amount,
    s.base_volume_24h,
    s.quote_volume_24h,
    s.contract_price,
    s.contract_price_currency,
    s.funding_rate,
    s.index_price,
    s.last_price,
    s.mark_price,
    s.next_funding_rate_timestamp,
    s.open_interest,
    s.open_interest_usd,
    s.price_change_percent_24h,
    s.product_type,
    s.quote_currency,
    s.quote_volume,
    t.modified_timestamp
FROM
    {{ this }}
    s
    LEFT JOIN trade_snapshot t
    ON t.ticker_id = s.ticker_id
    AND s.hour = t.hour
    LEFT JOIN products p
    ON s.ticker_id = p.ticker_id
{% endif %}
)
SELECT
    *,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['ticker_id','hour']
    ) }} AS vertex_market_stats_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL 
WHERE FUNDING_RATE <> 0  qualify(ROW_NUMBER() over(PARTITION BY ticker_id, HOUR
ORDER BY
    modified_timestamp DESC NULLS LAST)) = 1
