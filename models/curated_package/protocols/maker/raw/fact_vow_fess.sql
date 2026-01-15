{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    utils.udf_hex_to_int(topics[2])::double/1e45 as tab,
    tx_hash
FROM {{ ref('core__fact_event_logs') }}
where topics[0] = '0x697efb7800000000000000000000000000000000000000000000000000000000'
and contract_address = lower('0xA950524441892A31ebddF91d3cEEFa04Bf454466')