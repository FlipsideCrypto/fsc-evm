{%- set database = target.database.lower() | replace('_dev', '') -%}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = get_path_tags(model)
) }}

SELECT
    chain,
    key,
    parent_key,
    VALUE,
    is_enabled,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'key', 'parent_key']
    ) }} AS fact_variables_id
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_values'
    ) }}
WHERE
    chain = '{{ database }}'
