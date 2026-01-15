{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

with raw as (
SELECT
    trace_index,
    trace_address,
    block_timestamp,
    tx_hash,
    SUBSTR(input, 11) as raw_input_data
FROM {{ ref('core__fact_traces') }}
where to_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and left(input, 10) in ('0x509aaa1d', '0x26e027f1')
)
SELECT 
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 1, 64)) as _id,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 65, 64)) as _end
FROM raw