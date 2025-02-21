{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view'
) }}

SELECT
    m.index,
    v.chain,
    m.package,
    m.category,
    m.data_type,
    m.key,
    m.parent_key,
    v.value,
    IFNULL(
        v.is_enabled,
        FALSE
    ) AS is_enabled,
    {{ dbt_utils.generate_surrogate_key(
        ['v.chain', 'm.key', 'm.parent_key']
    ) }} AS ez_variables_id
FROM
    {{ ref(
        'silver__dim_variables'
    ) }}
    m
    INNER JOIN {{ ref(
        'silver__fact_variables'
    ) }}
    v
    ON m.key = v.key
    AND COALESCE(
        m.parent_key,
        'NULL'
    ) = COALESCE(
        v.parent_key,
        'NULL'
    )
