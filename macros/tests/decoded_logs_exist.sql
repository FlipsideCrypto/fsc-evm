{% test decoded_logs_exist(model, fact_logs_model) %}
SELECT
    d.block_number,
    d.ez_decoded_event_logs_id
FROM
    {{ model }}
    d
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            {{ fact_logs_model }}
            l
        WHERE
            d.ez_decoded_event_logs_id = l.fact_event_logs_id
            AND d.contract_address = l.contract_address
            AND d.topics [0] :: STRING = l.topics [0] :: STRING
    ) 
{% endtest %}