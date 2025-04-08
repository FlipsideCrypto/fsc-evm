{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver','admin','variables','phase_1']
) }}

SELECT
    f.project,
    d.PACKAGE,
    d.CATEGORY,
    f.key,
    f.value,
    f.parent_key,
    d.default_value,
    d.default_type,
    {{ dbt_utils.generate_surrogate_key(
        ['f.project', 'f.key', 'f.parent_key']
    ) }} AS ez_variables_id
FROM
    {{ ref('admin__fact_variables') }}
    f
    LEFT JOIN {{ ref('admin__dim_variables') }}
    d
    ON f.key = d.key
    OR f.parent_key = d.key
