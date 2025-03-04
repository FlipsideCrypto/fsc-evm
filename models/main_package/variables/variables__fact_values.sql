{%- set database = target.database.lower() | replace('_dev', '') -%}

{{ config(
    materialized = 'view',
    tags = ['vars']
) }}

SELECT
    chain,
    key,
    parent_key,
    VALUE,
    is_enabled,
    {{ dbt_utils.generate_surrogate_key(
        ['chain', 'key', 'parent_key']
    ) }} AS value_id
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_values'
    ) }}
WHERE
    chain = '{{ database }}'
