{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'chain'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'balancer', 'token_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Balancer Token Incentives All Chains

    Tracks BAL token incentives across Ethereum, Arbitrum, and Polygon from:
    - Merkle Redeem (Ethereum): 0x6d19b2bf3a36a61530909ae65445a906d98a2fa8
    - Merkle Orchard (Ethereum): 0xdae7e32adc5d490a43ccba1f0c736033f2b4efca
    - Merkle Orchard (Arbitrum): 0x751A0bC0e3f75b38e01Cf25bFCE7fF36DE1C87DE
    - Merkle Orchard (Polygon): 0x0f3e0c4218b7b0108a3643cfe9d3ec0d4f57c54e
#}

WITH merkle_redeem AS (
    SELECT
        block_timestamp::date AS date,
        price,
        decimals,
        decoded_log:_balance::NUMBER AS amount,
        decoded_log:_claimant::STRING AS recipient_address,
        (decoded_log:_balance::NUMBER / POW(10, decimals)) * price AS amount_usd
    FROM {{ ref('core__ez_decoded_event_logs') }}
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON LOWER('0xba100000625a3754423978a60c9317c58a424e3d') = token_address -- BAL token
        AND DATE_TRUNC('hour', block_timestamp) = hour
    WHERE contract_address = LOWER('0x6d19b2bf3a36a61530909ae65445a906d98a2fa8')
        AND event_name = 'Claimed'
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

merkle_orchard AS (
    SELECT
        block_timestamp::date AS date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER AS amount,
        decoded_log:claimer::STRING AS recipient_address,
        decoded_log:token::STRING AS token_address,
        (decoded_log:amount::NUMBER / POW(10, decimals)) * price AS amount_usd
    FROM {{ ref('core__ez_decoded_event_logs') }}
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON LOWER('0xba100000625a3754423978a60c9317c58a424e3d') = token_address -- BAL token
        AND DATE_TRUNC('hour', block_timestamp) = hour
    WHERE contract_address = LOWER('0xdae7e32adc5d490a43ccba1f0c736033f2b4efca')
        AND event_name = 'DistributionClaimed'
        AND decoded_log:token::STRING = LOWER('0xba100000625a3754423978a60c9317c58a424e3d')
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

merkle_orchard_arbitrum AS (
    SELECT
        block_timestamp::date AS date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER AS amount,
        decoded_log:claimer::STRING AS recipient_address,
        decoded_log:token::STRING AS token_address,
        LOWER(decoded_log:token::STRING) AS lower_token_address,
        (decoded_log:amount::NUMBER / POW(10, decimals)) * price AS amount_usd
    FROM {{ ref('core__ez_decoded_event_logs') }} a
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} t
        ON t.token_address = '0x040d1edc9569d4bab2d15287dc5a4f10f56a56b8' -- BAL token
        AND DATE_TRUNC('hour', block_timestamp) = hour
    WHERE contract_address = LOWER('0x751A0bC0e3f75b38e01Cf25bFCE7fF36DE1C87DE')
        AND event_name = 'DistributionClaimed'
        AND lower_token_address = '0x040d1edc9569d4bab2d15287dc5a4f10f56a56b8'
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

merkle_orchard_polygon AS (
    SELECT
        block_timestamp::date AS date,
        price,
        decimals,
        decoded_log,
        decoded_log:amount::NUMBER AS amount,
        decoded_log:claimer::STRING AS recipient_address,
        decoded_log:token::STRING AS token_address,
        (decoded_log:amount::NUMBER / POW(10, decimals)) * price AS amount_usd
    FROM {{ ref('core__ez_decoded_event_logs') }}
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON LOWER('0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3') = token_address -- BAL token
        AND DATE_TRUNC('hour', block_timestamp) = hour
    WHERE contract_address = LOWER('0x0f3e0c4218b7b0108a3643cfe9d3ec0d4f57c54e')
        AND event_name = 'DistributionClaimed'
        AND decoded_log:token::STRING = LOWER('0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3')
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

emissions AS (
    SELECT
        DATE(block_timestamp) AS date,
        'ethereum' AS chain,
        'BAL' AS token,
        to_address AS emission_contract,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE
        contract_address = LOWER('0xba100000625a3754423978a60c9317c58a424e3d') -- BAL token
        AND from_address = '0x0000000000000000000000000000000000000000' -- Zero address (minting)
        AND block_timestamp >= '2022-04-01'
    {% if is_incremental() %}
        AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

combined AS (
    SELECT
        date,
        'ethereum' AS chain,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM merkle_orchard
    GROUP BY date, chain

    UNION ALL

    SELECT
        date,
        'ethereum' AS chain,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM merkle_redeem
    GROUP BY date, chain

    UNION ALL

    SELECT
        date,
        'arbitrum' AS chain,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM merkle_orchard_arbitrum
    GROUP BY date, chain

    UNION ALL

    SELECT
        date,
        'polygon' AS chain,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM merkle_orchard_polygon
    GROUP BY date, chain

    UNION ALL

    SELECT
        date,
        'ethereum_emissions' AS chain,
        SUM(amount) AS amount,
        SUM(amount_usd) AS amount_usd
    FROM emissions
    GROUP BY date, chain
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM combined
