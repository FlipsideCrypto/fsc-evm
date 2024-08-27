{% macro retry_missing_txs() %}
    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    ),
    transactions AS (
        SELECT
            block_number,
            POSITION,
            LAG(
                POSITION,
                1
            ) over (
                PARTITION BY block_number
                ORDER BY
                    POSITION ASC
            ) AS prev_POSITION
        FROM
            {{ ref("silver__transactions") }}
        WHERE
            block_timestamp >= DATEADD('hour', -84, SYSDATE())
            AND block_number >= (
                SELECT
                    block_number
                FROM
                    lookback
            )
    )
SELECT
    DISTINCT block_number AS block_number
FROM
    transactions
WHERE
    POSITION - prev_POSITION <> 1
{% endmacro %}

{% macro retry_missing_receipts() %}
    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
SELECT
    DISTINCT t.block_number AS block_number
FROM
    {{ ref("silver__transactions") }}
    t
    LEFT JOIN {{ ref("silver__receipts") }}
    r USING (
        block_number,
        block_hash,
        tx_hash
    )
WHERE
    r.tx_hash IS NULL
    AND t.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND t.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND (
        r._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR r._inserted_timestamp IS NULL)
{% endmacro %}

{% macro retry_missing_traces() %}
    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
SELECT
    DISTINCT tx.block_number block_number
FROM
    {{ ref("silver__transactions") }}
    tx
    LEFT JOIN {{ ref("silver__traces") }}
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
    AND tr.block_timestamp >= DATEADD('hour', -84, SYSDATE())
WHERE
    tx.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.tx_hash IS NULL
    AND tx.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
{% endmacro %}

{% macro retry_unconfirmed_blocks() %}
    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
SELECT
    DISTINCT cb.block_number AS block_number
FROM
    {{ ref("silver__confirmed_blocks") }}
    cb
    LEFT JOIN {{ ref("silver__transactions") }}
    txs USING (
        block_number,
        block_hash,
        tx_hash
    )
WHERE
    txs.tx_hash IS NULL
    AND cb.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND cb._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND (
        txs._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR txs._inserted_timestamp IS NULL)
{% endmacro %}
