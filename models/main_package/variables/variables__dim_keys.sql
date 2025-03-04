{{ config(
    materialized = 'view',
    tags = ['vars']
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
    ) }} AS key_id
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_keys'
    ) }}
