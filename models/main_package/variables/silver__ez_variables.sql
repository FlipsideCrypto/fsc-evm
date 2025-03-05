{{ config(
    materialized = 'view',
    tags = ['project_vars']
) }}

{%- set project = target.database.lower() | replace('_dev', '') -%}

SELECT
    package,
    category,
    f.key,
    VALUE,
    parent_key,
    data_type,
    default_value
FROM
    {{ ref('silver__fact_variables') }} f
INNER JOIN
    {{ ref('silver__dim_variables') }} d
    ON f.key = d.key
WHERE
    project = '{{ project }}'
