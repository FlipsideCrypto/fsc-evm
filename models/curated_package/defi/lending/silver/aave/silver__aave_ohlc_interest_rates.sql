{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['hour', 'protocol', 'platform', 'version', 'token_address'],
    cluster_by = ['hour::DATE', 'protocol', 'platform'],
    tags = ['silver','defi','lending','curated','aave','ohlc']
) }}

WITH hourly_rates AS (
    SELECT
        DATE_TRUNC('hour', block_timestamp) AS hour,
        protocol,
        platform,
        version,
        token_address,
        token_symbol,
        liquidity_rate,
        stable_borrow_rate,
        variable_borrow_rate,
        block_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__aave_interest_rates') }}
    WHERE
        liquidity_rate IS NOT NULL
        OR stable_borrow_rate IS NOT NULL
        OR variable_borrow_rate IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
rate_aggregations AS (
    SELECT
        hour,
        protocol,
        platform,
        version,
        token_address,
        token_symbol,
        -- Liquidity Rate OHLC
        MIN(liquidity_rate) AS liquidity_rate_low,
        MAX(liquidity_rate) AS liquidity_rate_high,
        FIRST_VALUE(liquidity_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS liquidity_rate_open,
        LAST_VALUE(liquidity_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS liquidity_rate_close,
        -- Stable Borrow Rate OHLC
        MIN(stable_borrow_rate) AS stable_borrow_rate_low,
        MAX(stable_borrow_rate) AS stable_borrow_rate_high,
        FIRST_VALUE(stable_borrow_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS stable_borrow_rate_open,
        LAST_VALUE(stable_borrow_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS stable_borrow_rate_close,
        -- Variable Borrow Rate OHLC
        MIN(variable_borrow_rate) AS variable_borrow_rate_low,
        MAX(variable_borrow_rate) AS variable_borrow_rate_high,
        FIRST_VALUE(variable_borrow_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS variable_borrow_rate_open,
        LAST_VALUE(variable_borrow_rate) OVER (
            PARTITION BY hour, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS variable_borrow_rate_close,
        COUNT(*) AS rate_updates_count,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        hourly_rates
    GROUP BY
        hour,
        protocol,
        platform,
        version,
        token_address,
        token_symbol
)
SELECT
    hour,
    protocol,
    platform,
    version,
    token_address,
    token_symbol,
    -- Liquidity Rate OHLC
    liquidity_rate_open,
    liquidity_rate_high,
    liquidity_rate_low,
    liquidity_rate_close,
    -- Stable Borrow Rate OHLC
    stable_borrow_rate_open,
    stable_borrow_rate_high,
    stable_borrow_rate_low,
    stable_borrow_rate_close,
    -- Variable Borrow Rate OHLC
    variable_borrow_rate_open,
    variable_borrow_rate_high,
    variable_borrow_rate_low,
    variable_borrow_rate_close,
    rate_updates_count,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS blockchain,
    {{ dbt_utils.generate_surrogate_key(
        ['hour', 'protocol', 'platform', 'version', 'token_address']
    ) }} AS aave_interest_rates_ohlc_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    rate_aggregations
WHERE
    (liquidity_rate_open IS NOT NULL OR liquidity_rate_high IS NOT NULL OR liquidity_rate_low IS NOT NULL OR liquidity_rate_close IS NOT NULL)
    OR (stable_borrow_rate_open IS NOT NULL OR stable_borrow_rate_high IS NOT NULL OR stable_borrow_rate_low IS NOT NULL OR stable_borrow_rate_close IS NOT NULL)
    OR (variable_borrow_rate_open IS NOT NULL OR variable_borrow_rate_high IS NOT NULL OR variable_borrow_rate_low IS NOT NULL OR variable_borrow_rate_close IS NOT NULL)
qualify(ROW_NUMBER() over(PARTITION BY hour, protocol, platform, version, token_address
ORDER BY
    modified_timestamp DESC)) = 1 