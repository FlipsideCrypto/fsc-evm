{%- set database = target.database.lower() | replace('_dev', '') -%}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view'
) }}

SELECT
    chain,
    key,
    parent_key,
    VALUE,
    is_enabled
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_values'
    ) }}
WHERE
    chain = '{{ database }}'
