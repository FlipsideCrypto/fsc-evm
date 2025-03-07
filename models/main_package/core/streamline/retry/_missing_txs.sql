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
        WITH transactions AS (
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
                {{ ref("test_gold__fact_transactions_recent") }}
            WHERE
                block_timestamp > DATEADD('day', -5, SYSDATE())
        )
    SELECT
        DISTINCT block_number AS block_number
    FROM
        transactions
    WHERE
        tx_position - prev_tx_position <> 1
    {% endif %}
