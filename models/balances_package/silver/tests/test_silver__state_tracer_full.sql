{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','balances','full_test','phase_4']
) }}

SELECT
    *
FROM
    {{ ref('silver__state_tracer') }}
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED %}
WHERE
    block_number NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}