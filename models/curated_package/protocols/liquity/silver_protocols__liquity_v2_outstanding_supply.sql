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
    Liquity V2 Outstanding BOLD Supply

    Tracks the net BOLD supply on Ethereum by monitoring mints and burns
    BOLD contract: 0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98
#}

WITH daily_change AS (
    SELECT
        block_timestamp::DATE AS date
        , SUM(
            CASE
                WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount
                WHEN to_address = '0x0000000000000000000000000000000000000000' THEN -1 * amount
            END
        ) AS net
        , MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1
),

sparse AS (
    SELECT
        ds.date
        , c.net
        , c.modified_timestamp
    FROM {{ ref('dim_date_spine') }} ds
    LEFT JOIN daily_change c USING(date)
    WHERE ds.date BETWEEN (SELECT MIN(date) FROM daily_change) AND TO_DATE(SYSDATE())
)

SELECT
    date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , 'v2' AS version
    , 'BOLD' AS token
    , SUM(net) OVER (ORDER BY date ASC) AS outstanding_supply
    , MAX(modified_timestamp) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM sparse
