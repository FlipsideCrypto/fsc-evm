{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    utils.udf_hex_to_string(rtrim(topics[2], 0)) as ilk,
    tx_hash
FROM {{ ref('core__fact_event_logs') }}
where topics[0] = '0x1a0b287e00000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x65C79fcB50Ca1594B025960e539eD7A9a6D434A3'