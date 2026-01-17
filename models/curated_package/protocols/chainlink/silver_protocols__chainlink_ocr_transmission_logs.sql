{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'ocr', 'transmission', 'curated']
) }}

{#
    Chainlink OCR Transmission Logs

    Captures OCR (Off-Chain Reporting) transmission events.
    Topic hash: 0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6

    This model works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.
#}

WITH base_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        data,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        modified_timestamp,
        CONCAT(tx_hash, '-', event_index) AS _log_id
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string = '0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    topics,
    data,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    _log_id,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base_logs
