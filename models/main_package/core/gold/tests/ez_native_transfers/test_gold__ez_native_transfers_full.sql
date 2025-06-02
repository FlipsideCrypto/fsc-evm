{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','core','full_test','transfers','ez','phase_3']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_native_transfers') }}
{% if vars.MAIN_OBSERV_EXCLUSION_LIST_ENABLED %}
WHERE
    block_number NOT IN (
        SELECT
            block_number :: INT
        FROM
            observability.exclusion_list
    )
{% endif %}