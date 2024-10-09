{{ config(
    materialized = 'ephemeral'
) }}
    
{% set new_build = var('BLOCKS_TRANSACTIONS_REALTIME_NEW_BUILD', False) %}

{% if new_build %}

SELECT  
    -1 AS block_number

{% else %}
    
    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    ),
    transactions AS (
        SELECT
            block_number,
            tx_position,
            LAG(
                tx_position,
                1
            ) over (
                PARTITION BY block_number
                ORDER BY
                    tx_position ASC
            ) AS prev_tx_position
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
    tx_position - prev_tx_position <> 1

{% endif %}