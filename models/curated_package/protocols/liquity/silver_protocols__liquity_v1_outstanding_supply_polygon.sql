{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'outstanding_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity V1 Outstanding LUSD Supply - Polygon

    Tracks the net LUSD supply on Polygon by monitoring mints and burns
    LUSD contract on Polygon: 0x23001f892c0C82b79303EDC9B9033cD190BB21c7
#}

WITH supply_changes AS (
    SELECT
        block_timestamp::DATE AS date
        , SUM(
            CASE
                WHEN from_address = '0x0000000000000000000000000000000000000000'
                    THEN raw_amount_precise::NUMBER / 1e18
                ELSE -(raw_amount_precise::NUMBER / 1e18)
            END
        ) AS lusd_supply_change
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0x23001f892c0C82b79303EDC9B9033cD190BB21c7')
    AND (
        from_address = '0x0000000000000000000000000000000000000000'
        OR to_address = '0x0000000000000000000000000000000000000000'
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1
),

date_spine AS (
    SELECT date
    FROM {{ ref('utils__date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM supply_changes) AND TO_DATE(SYSDATE())
),

all_dates AS (
    SELECT
        ds.date
        , c.lusd_supply_change
        , c.modified_timestamp
    FROM date_spine ds
    LEFT JOIN supply_changes c USING(date)
)

SELECT
    date
    , 'polygon' AS chain
    , 'Liquity' AS protocol
    , 'v1' AS version
    , 'LUSD' AS token
    , SUM(lusd_supply_change) OVER (ORDER BY date ASC) AS outstanding_supply
    , MAX(modified_timestamp) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM all_dates
