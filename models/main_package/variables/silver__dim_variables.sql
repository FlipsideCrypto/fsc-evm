{{ config(
    materialized = 'view',
    tags = ['silver_vars']
) }}

SELECT
    INDEX,
    PACKAGE,
    category,
    data_type,
    key,
    parent_key,
    default_value,
    {{ dbt_utils.generate_surrogate_key(
        ['key', 'parent_key']
    ) }} AS dim_variables_id
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_keys'
    ) }}
