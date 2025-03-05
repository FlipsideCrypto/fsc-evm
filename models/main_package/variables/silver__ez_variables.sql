{{ config(
    materialized = 'view',
    tags = ['silver_vars']
) }}

SELECT
    PACKAGE,
    category,
    f.key,
    VALUE,
    parent_key,
    data_type,
    default_value,
    {{ dbt_utils.generate_surrogate_key(
        ['f.key', 'f.parent_key']
    ) }} AS ez_variables_id
FROM
    {{ ref('silver__fact_variables') }}
    f
    LEFT JOIN {{ ref('silver__dim_variables') }}
    d
    ON f.key = d.key
    OR f.parent_key = d.key
