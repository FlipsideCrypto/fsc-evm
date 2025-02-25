{{ config(
    materialized = 'ephemeral'
) }}

    SELECT
        DISTINCT block_number
    FROM
        {{ ref("test_gold__fact_transactions_recent") }}
    WHERE
        tx_succeeded IS NULL