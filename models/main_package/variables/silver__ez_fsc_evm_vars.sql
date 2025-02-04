{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view'
) }}

SELECT
    var_id,
    chain,
    category,
    sub_category,
    data_type,
    parent_key,
    key,
    VALUE,
    is_required,
    CASE
        WHEN is_enabled IS NULL THEN FALSE
        ELSE is_enabled
    END AS is_enabled
FROM
    {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_master_config'
    ) }} C
    INNER JOIN {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_chain_values'
    ) }}
    v USING (var_id)
