{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var('CONFIRM_BLOCKS_REALTIME_NEW_BUILD', false) %}

{% if new_build %}

SELECT  
    -1 AS block_number

{% else %}

    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
SELECT
    DISTINCT cb.block_number AS block_number
FROM
    {{ ref("silver__confirm_blocks") }}
    cb
    LEFT JOIN {{ ref("core__fact_transactions") }}
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
        txs.modified_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR txs.modified_timestamp IS NULL)

{% endif %}