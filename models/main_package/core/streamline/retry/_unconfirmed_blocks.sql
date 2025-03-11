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