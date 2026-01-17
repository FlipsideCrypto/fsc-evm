{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'operator_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'rewards', 'ocr', 'curated']
) }}

{#
    Chainlink OCR Rewards Daily

    Aggregates daily OCR (Off-Chain Reporting) transmission rewards by operator.
    OCR transmissions are identified by topic hash: 0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6

    This model works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.
#}

WITH ocr_transmissions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        data,
        origin_from_address,
        modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string = '0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
),

token_transfers AS (
    SELECT
        block_timestamp::date AS date,
        from_address,
        to_address,
        amount,
        contract_address AS token_address,
        tx_hash,
        modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE tx_hash IN (SELECT DISTINCT tx_hash FROM ocr_transmissions)
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    'OCR' AS reward_type,
    to_address AS operator_address,
    token_address,
    SUM(amount) AS reward_amount,
    COUNT(DISTINCT tx_hash) AS tx_count,
    MAX(modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM token_transfers
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
