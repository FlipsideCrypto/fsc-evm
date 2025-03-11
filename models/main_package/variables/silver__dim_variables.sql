{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['silver_vars']
) }}

SELECT
    PACKAGE,
    category,
    variable_key AS key,
    default_value,
    default_type,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['variable_key']
    ) }} AS dim_variables_id
FROM
    {{ source(
        'fsc_evm_bronze',
        '_master_keys'
    ) }}
    qualify(ROW_NUMBER() over (PARTITION BY variable_key
ORDER BY
    _inserted_timestamp DESC)) = 1
