{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'chain'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'dai_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH arb_raw AS (
    SELECT
        block_timestamp,
        CASE
            WHEN LOWER(FROM_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000') THEN AMOUNT
            WHEN LOWER(TO_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000') THEN -AMOUNT
        END AS amount
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        LOWER(contract_address) = LOWER('0xda10009cbd5d07dd0cecc66161fc93d7c9000da1')
        AND (
            LOWER(FROM_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000')
            OR LOWER(TO_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000')
        )
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

daily_amounts AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(amount) AS daily_amount
    FROM arb_raw
    GROUP BY DATE(block_timestamp)
)

SELECT
    date,
    SUM(daily_amount) OVER (ORDER BY date) AS dai_supply,
    'Arbitrum' AS chain,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM daily_amounts
