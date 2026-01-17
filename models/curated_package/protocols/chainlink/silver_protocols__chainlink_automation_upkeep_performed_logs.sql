{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'automation', 'curated']
) }}

{#
    Chainlink Automation Upkeep Performed Logs

    Captures Automation (formerly Keepers) upkeep performed events.
    Topic hashes:
    - 0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6 (UpkeepPerformed)
    - 0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b (UpkeepPerformedV2)

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
        '0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6',
        '0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b'
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
