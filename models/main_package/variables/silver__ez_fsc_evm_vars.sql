{{ config(
    materialized = 'view'
) }}

SELECT
    var_id,
    category,
    sub_category,
    data_type,
    parent_key,
    key,
    VALUE,
    is_required,
    is_enabled
FROM
    {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_master_config'
    ) }} C
    LEFT JOIN {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_chain_values'
    ) }}
    v USING (var_id)
