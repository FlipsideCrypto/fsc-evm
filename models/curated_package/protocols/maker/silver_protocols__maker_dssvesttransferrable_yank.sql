{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash', '_id'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'dssvest', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH raw AS (
    SELECT
        trace_index,
        trace_address,
        block_timestamp,
        tx_hash,
        SUBSTR(input, 11) AS raw_input_data
    FROM {{ ref('core__fact_traces') }}
    WHERE to_address = LOWER('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
    AND LEFT(input, 10) IN ('0x509aaa1d', '0x26e027f1')
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 1, 64)) AS _id,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 65, 64)) AS _end,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM raw
