{{ config(
    materialized = 'view',
    tags = ['project_vars']
) }}

{%- set project = target.database.lower() | replace('_dev', '') -%}

SELECT
    key,
    VALUE,
    parent_key
FROM
    {{ ref('silver__ez_variables_all') }}
WHERE
    project = '{{ project }}'
