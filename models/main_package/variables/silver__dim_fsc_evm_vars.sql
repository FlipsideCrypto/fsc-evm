{# Log configuration details #}
{{ log_model_details() }}

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
    is_required
FROM
    {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_master_config'
    ) }}
