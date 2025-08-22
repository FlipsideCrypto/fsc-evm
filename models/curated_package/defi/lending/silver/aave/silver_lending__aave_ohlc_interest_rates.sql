{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['day', 'protocol', 'platform', 'version', 'token_address'],
    cluster_by = ['day::DATE', 'protocol', 'platform'],
    tags = ['silver','defi','lending','curated','aave','interest_rates','ohlc']
) }}

WITH daily_rates AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        protocol,
        platform,
        version,
        token_address,
        supply_rate_unadj AS supply_rate,
        stable_borrow_rate_unadj AS stable_borrow_rate,
        variable_borrow_rate_unadj AS variable_borrow_rate,
        block_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_lending__aave_interest_rates') }}
    WHERE
        supply_rate_unadj IS NOT NULL
        OR stable_borrow_rate_unadj IS NOT NULL
        OR variable_borrow_rate_unadj IS NOT NULL

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
window_calculations AS (
    SELECT
        day,
        protocol,
        platform,
        version,
        token_address,
        supply_rate,
        stable_borrow_rate,
        variable_borrow_rate,
        FIRST_VALUE(supply_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS supply_rate_open,
        LAST_VALUE(supply_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS supply_rate_close,
        FIRST_VALUE(stable_borrow_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS stable_borrow_rate_open,
        LAST_VALUE(stable_borrow_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS stable_borrow_rate_close,
        FIRST_VALUE(variable_borrow_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
        ) AS variable_borrow_rate_open,
        LAST_VALUE(variable_borrow_rate) OVER (
            PARTITION BY day, protocol, platform, version, token_address 
            ORDER BY block_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS variable_borrow_rate_close,
        modified_timestamp
    FROM
        daily_rates
),
rate_aggregations AS (
    SELECT
        day,
        protocol,
        platform,
        version,
        token_address,
        -- Supply Rate OHLC
        MIN(supply_rate) AS supply_rate_low,
        MAX(supply_rate) AS supply_rate_high,
        MAX(supply_rate_open) AS supply_rate_open,
        MAX(supply_rate_close) AS supply_rate_close,
        -- Stable Borrow Rate OHLC
        MIN(stable_borrow_rate) AS stable_borrow_rate_low,
        MAX(stable_borrow_rate) AS stable_borrow_rate_high,
        MAX(stable_borrow_rate_open) AS stable_borrow_rate_open,
        MAX(stable_borrow_rate_close) AS stable_borrow_rate_close,
        -- Variable Borrow Rate OHLC
        MIN(variable_borrow_rate) AS variable_borrow_rate_low,
        MAX(variable_borrow_rate) AS variable_borrow_rate_high,
        MAX(variable_borrow_rate_open) AS variable_borrow_rate_open,
        MAX(variable_borrow_rate_close) AS variable_borrow_rate_close,
        COUNT(*) AS rate_updates_count,
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        window_calculations
    GROUP BY
        day,
        protocol,
        platform,
        version,
        token_address
),
-- Fill missing days with forward fill using window functions
filled_rates AS (
    SELECT
        day,
        protocol,
        platform,
        version,
        token_address,
        -- Forward fill supply rate values using LAST_VALUE to get the last known value
        COALESCE(
            supply_rate_open,
            LAST_VALUE(supply_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS supply_rate_open,
        COALESCE(
            supply_rate_high,
            LAST_VALUE(supply_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS supply_rate_high,
        COALESCE(
            supply_rate_low,
            LAST_VALUE(supply_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS supply_rate_low,
        COALESCE(
            supply_rate_close,
            LAST_VALUE(supply_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS supply_rate_close,
        -- Forward fill stable borrow rate values
        COALESCE(
            stable_borrow_rate_open,
            LAST_VALUE(stable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS stable_borrow_rate_open,
        COALESCE(
            stable_borrow_rate_high,
            LAST_VALUE(stable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS stable_borrow_rate_high,
        COALESCE(
            stable_borrow_rate_low,
            LAST_VALUE(stable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS stable_borrow_rate_low,
        COALESCE(
            stable_borrow_rate_close,
            LAST_VALUE(stable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS stable_borrow_rate_close,
        -- Forward fill variable borrow rate values
        COALESCE(
            variable_borrow_rate_open,
            LAST_VALUE(variable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS variable_borrow_rate_open,
        COALESCE(
            variable_borrow_rate_high,
            LAST_VALUE(variable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS variable_borrow_rate_high,
        COALESCE(
            variable_borrow_rate_low,
            LAST_VALUE(variable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS variable_borrow_rate_low,
        COALESCE(
            variable_borrow_rate_close,
            LAST_VALUE(variable_borrow_rate_close) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS variable_borrow_rate_close,
        COALESCE(rate_updates_count, 0) AS rate_updates_count,
        COALESCE(
            modified_timestamp,
            LAST_VALUE(modified_timestamp) IGNORE NULLS OVER (
                PARTITION BY protocol, platform, version, token_address
                ORDER BY day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) AS modified_timestamp
    FROM
        rate_aggregations
)
SELECT
    day,
    protocol,
    platform,
    version,
    token_address,
    -- Supply Rate OHLC
    supply_rate_open,
    supply_rate_high,
    supply_rate_low,
    supply_rate_close,
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
        ['day', 'protocol', 'platform', 'version', 'token_address']
    ) }} AS aave_interest_rates_ohlc_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    filled_rates 