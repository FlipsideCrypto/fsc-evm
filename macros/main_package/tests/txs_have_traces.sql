{% test txs_have_traces(
    model,
    transactions_model
) %}

{% set vars = return_vars() %}

WITH txs_with_traces AS (

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
    (
        tr.tx_hash IS NULL
        OR tr.tx_position IS NULL
        OR tr.block_number IS NULL
    )
    AND txs.from_address <> '0x0000000000000000000000000000000000000000'
    AND txs.to_address <> '0x0000000000000000000000000000000000000000' 
    {% if vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
        AND txs.to_address <> '0x000000000000000000000000000000000000006e'
        AND txs.block_number > 22207817
    {% endif %}
    {% if vars.GLOBAL_PROJECT_NAME == 'boba' %}
        AND txs.block_number > 1041894
    {% endif %}
)

SELECT
    *
FROM
    txs_with_traces
WHERE
    (
        SELECT
            COUNT(DISTINCT block_number) >= {{ vars.MAIN_CORE_GOLD_TRACES_TEST_ERROR_THRESHOLD }}
        FROM
            txs_with_traces
    )

{% endtest %}
