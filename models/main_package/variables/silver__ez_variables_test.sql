{{ config(
    materialized = 'view'
) }}

{%- set project = target.database.lower() | replace('_dev', '') -%}

SELECT
    key,
    VALUE,
    parent_key
FROM
    {{ ref('silver__ez_variables_test2') }}
WHERE
    project = '{{ project }}'
