{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT 
    block_timestamp,
    utils.udf_hex_to_string(rtrim(topics[2],0)) as ilk,
    tx_hash
FROM {{ ref('core__fact_event_logs') }}
where topics[0] = '0x29ae811400000000000000000000000000000000000000000000000000000000'
and contract_address ilike '0x19c0976f590D67707E62397C87829d896Dc0f1F1'