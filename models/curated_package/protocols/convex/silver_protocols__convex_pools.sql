{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'lptoken'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'convex', 'pools', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Pools

    Tracks Convex pool additions via addPool function calls to the Booster contract.
    Booster address: 0xF403C135812408BFbE8713b5A23a04b3D48AAE31
#}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    decoded_input_data:_lptoken::STRING AS lptoken,
    decoded_input_data:_gauge::STRING AS gauge,
    decoded_input_data:_stashVersion::NUMBER AS stash_version,
    ROW_NUMBER() OVER (ORDER BY block_number) - 1 AS pid,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_decoded_traces') }}
WHERE to_address = LOWER('0xF403C135812408BFbE8713b5A23a04b3D48AAE31')
    AND function_name = 'addPool'
    AND trace_succeeded = TRUE
{% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
