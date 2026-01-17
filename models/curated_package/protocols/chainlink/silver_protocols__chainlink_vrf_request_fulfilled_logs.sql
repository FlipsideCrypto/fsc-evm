{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'vrf', 'curated']
) }}

{#
    Chainlink VRF Request Fulfilled Logs

    Captures VRF (Verifiable Random Function) request and fulfillment events.
    Topic hashes:
    - 0x63373d1c4696214b898952999c9aaec57dac1ee2723cec59bea6888f489a9772 (RandomWordsRequest)
    - 0x7dffc5ae5ee4e2e4df1651cf6ad329a73cebdb728f37ea0187b9b17e036756e4 (RandomWordsFulfilled)

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
    WHERE topics[0]::string IN (
        '0x63373d1c4696214b898952999c9aaec57dac1ee2723cec59bea6888f489a9772',
        '0x7dffc5ae5ee4e2e4df1651cf6ad329a73cebdb728f37ea0187b9b17e036756e4'
    )
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
