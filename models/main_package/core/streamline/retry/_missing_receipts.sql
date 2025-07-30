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
        DISTINCT block_number
    FROM
        {{ ref("test_gold__fact_transactions_recent") }}
    WHERE
        tx_succeeded IS NULL
{% endif %}