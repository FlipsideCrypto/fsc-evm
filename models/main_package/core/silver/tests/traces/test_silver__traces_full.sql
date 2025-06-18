{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','core','full_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__traces') }}
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED %}
WHERE
    block_number NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}