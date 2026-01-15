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
and left(input, 10) = '0xdb64ff8f'
)
SELECT
    block_timestamp,
    tx_hash,
    '0x' || SUBSTR(raw_input_data, 25, 40) as _usr,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 65, 64)) as _tot,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 129, 64)) as _bgn,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 193, 64)) as _tau,
    utils.udf_hex_to_int(SUBSTR(raw_input_data, 257, 64)) as _eta,
    ROW_NUMBER() OVER (ORDER BY block_timestamp, trace_index) AS output_id
FROM raw