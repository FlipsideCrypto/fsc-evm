{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view'
) }}

SELECT
    m.var_id,
    v.chain,
    m.category,
    m.sub_category,
    m.data_type,
    m.key,
    m.parent_key,
    v.value,
    m.is_required,
    IFNULL(
        v.is_enabled,
        FALSE
    ) AS is_enabled
FROM
    {{ ref(
        'silver__dim_fsc_evm_vars'
    ) }}
    m
    INNER JOIN {{ ref(
        'silver__fact_fsc_evm_vars'
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
