{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'convex', 'revenue', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Revenue

    Tracks daily revenue from CRV and 3CRV token transfers.
    CRV mints to Voter Proxy: 0x989aeb4d175e16225e39e87d0d97a3360524ad80
    3CRV transfers to Fee Distributor: 0x7091dbb7fcbA54569eF1387Ac89Eb2a5C9F6d2EA

    Revenue split: 17% protocol revenue, 83% supply side fees
#}

WITH transfers AS (
    SELECT
        block_timestamp::DATE AS date,
        contract_address,
        SUM(raw_amount_precise) AS claimed,
        MAX(block_number) AS block_number,
        MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52')
        AND from_address = LOWER('0x0000000000000000000000000000000000000000')
        AND to_address = LOWER('0x989aeb4d175e16225e39e87d0d97a3360524ad80')
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2

    UNION ALL

    SELECT
        block_timestamp::DATE AS date,
        contract_address,
        SUM(raw_amount_precise) AS claimed,
        MAX(block_number) AS block_number,
        MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490')
        AND to_address = LOWER('0x7091dbb7fcbA54569eF1387Ac89Eb2a5C9F6d2EA')
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
            FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
    GROUP BY 1, 2
),

date_address_spine AS (
    SELECT DISTINCT
        ds.date,
        t.contract_address
    FROM {{ ref('dim_date_spine') }} ds
    JOIN transfers t
    WHERE ds.date BETWEEN (
            SELECT MIN(date)
            FROM transfers
        )
        AND TO_DATE(SYSDATE())
)

SELECT
    das.date,
    p.symbol AS token,
    'ethereum' AS chain,
    SUM((t.claimed / POWER(10, p.decimals)) * p.price) AS fees,
    SUM((t.claimed / POWER(10, p.decimals)) * p.price) * 0.17 AS revenue,
    SUM((t.claimed / POWER(10, p.decimals)) * p.price) * 0.83 AS primary_supply_side_fees,
    MAX(t.block_number) AS block_number,
    MAX(t.modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM date_address_spine AS das
LEFT JOIN transfers AS t
    ON t.date = das.date
    AND t.contract_address = das.contract_address
LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON p.token_address = t.contract_address
    AND p.hour = das.date
GROUP BY 1, 2
HAVING SUM((t.claimed / POWER(10, p.decimals)) * p.price) IS NOT NULL
