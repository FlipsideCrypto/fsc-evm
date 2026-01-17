{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'ccip', 'v1', 'curated']
) }}

{#
    Chainlink CCIP Send Requested Logs v1

    Captures CCIP (Cross-Chain Interoperability Protocol) v1 send requested events.
    Topic hash: 0xaffc45517195d6499808c643bd4a7b0ffeedf95bea5852840d7bfcf63f59e821

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
    WHERE topics[0]::string = '0xaffc45517195d6499808c643bd4a7b0ffeedf95bea5852840d7bfcf63f59e821'
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
