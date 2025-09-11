{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','balances','erc20','daily','daily_test','phase_4']
) }}

SELECT
    *
FROM
    {{ ref('balances__ez_balances_erc20_daily') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED %}
AND
    block_number NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}