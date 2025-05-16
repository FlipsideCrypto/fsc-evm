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
    (tr.tx_hash IS NULL
    OR tr.tx_position IS NULL
    OR tr.block_number IS NULL) 
    and txs.from_address <> '0x0000000000000000000000000000000000000000' 
    and txs.to_address <> '0x0000000000000000000000000000000000000000'
{% endtest %}