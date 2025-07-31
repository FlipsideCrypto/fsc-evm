{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% if vars.MAIN_SL_NEW_BUILD_ENABLED %}

    SELECT
        -1 AS block_number
    WHERE 0=1
{% else %}
    SELECT
        DISTINCT cb.block_number AS block_number
    FROM
        {{ ref("test_silver__confirm_blocks_recent") }}
        cb
        LEFT JOIN {{ ref("test_gold__fact_transactions_recent") }}
        txs 
        ON cb.block_number = txs.block_number
        and cb.tx_hash = txs.tx_hash
        and cb.partition_key = round(txs.block_number,-3)
    WHERE
        txs.tx_hash IS NULL
        AND cb.modified_timestamp > DATEADD('day', -5, SYSDATE())
{% endif %}