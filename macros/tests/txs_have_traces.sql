{% test txs_have_traces(
    model,
    transactions_model
) %}
SELECT
    block_number,
    tx_hash,
    tx_position
FROM
    {{ transactions_model }}
    txs
    LEFT JOIN {{ model }}
    tr USING (
        block_number,
        tx_hash,
        tx_position
    )
WHERE
    tr.tx_hash IS NULL
    OR tr.tx_position IS NULL
    OR tr.block_number IS NULL 
{% endtest %}