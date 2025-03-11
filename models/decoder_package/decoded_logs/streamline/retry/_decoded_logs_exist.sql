{% macro decoded_logs_exist(model, fact_logs_model) %}
SELECT
    d.block_number,
    d.tx_hash,
    d.event_index,
    d.contract_address,
    d.topics,
    d.data,
    CONCAT(
        d.tx_hash :: STRING,
        '-',
        d.event_index :: STRING
    ) AS _log_id
FROM
    {{ ref('test_gold__ez_decoded_event_logs') }}
    d
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            {{ ref('test_gold__fact_event_logs_recent') }}
            l
        WHERE
            d.ez_decoded_event_logs_id = l.fact_event_logs_id
            AND d.contract_address = l.contract_address
            AND d.topics [0] :: STRING = l.topics [0] :: STRING
    ) 
{% endmacro %}