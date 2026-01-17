{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'contract_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'token_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer Token Incentives

    Tracks BAL token incentives distributed from:
    - Merkle Redeem contract: 0x6d19b2bf3a36a61530909ae65445a906d98a2fa8
    - BAL token: 0xba100000625a3754423978a60c9317c58a424e3D
#}

WITH base AS (
    SELECT
        block_timestamp::date AS date,
        contract_address,
        p.symbol AS token,
        SUM(raw_amount_precise::number / 1e18) AS amount_native,
        SUM(raw_amount_precise::number / 1e18 * p.price) AS amount_usd
    FROM {{ ref('core__ez_token_transfers') }} t
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON p.token_address = t.contract_address
        AND p.hour = t.block_timestamp::date
    WHERE 1=1
        AND from_address = '0x6d19b2bf3a36a61530909ae65445a906d98a2fa8'
        AND contract_address = LOWER('0xba100000625a3754423978a60c9317c58a424e3D')
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY 1, 2, 3
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
