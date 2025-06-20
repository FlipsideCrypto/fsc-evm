{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','balances','full_test','phase_4']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_native_balances') }}
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED %}
WHERE
    block_number NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}