{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var(
    'CONFIRM_BLOCKS_REALTIME_NEW_BUILD',
    false
) %}
{% if new_build %}

    SELECT
        -1 AS block_number
{% else %}
    SELECT
        DISTINCT cb.block_number AS block_number
    FROM
        {{ ref("test_silver__confirm_blocks_recent") }}
        cb
        LEFT JOIN {{ ref("test_gold__fact_transactions_recent") }}
        txs USING (
            block_number,
            tx_hash
        )
    WHERE
        txs.tx_hash IS NULL
        AND cb.modified_timestamp > DATEADD('day', -5, SYSDATE())
{% endif %}