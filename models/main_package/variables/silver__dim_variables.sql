{# Log configuration details #}
{{ log_model_details() }}

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
    {{ dbt_utils.generate_surrogate_key(
        ['key', 'parent_key']
    ) }} AS dim_variables_id
FROM
    {{ source(
        'fsc_evm_bronze',
        'master_variable_keys'
    ) }}
