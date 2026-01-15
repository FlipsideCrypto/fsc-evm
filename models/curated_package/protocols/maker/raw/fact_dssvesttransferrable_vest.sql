{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    tx_hash,
    utils.udf_hex_to_int(topics[1]) as _id,
    utils.udf_hex_to_int(data) as _max_amt
FROM {{ ref('core__fact_event_logs') }}
where contract_address = lower('0x6D635c8d08a1eA2F1687a5E46b666949c977B7dd')
and topics[0] = '0xa2906882572b0e9dfe893158bb064bc308eb1bd87d1da481850f9d17fc293847'