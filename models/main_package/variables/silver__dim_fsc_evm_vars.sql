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
    key,
    parent_key,
    is_required
FROM
    {{ ref(
        'fsc_evm_bronze',
        'master_variable_keys'
    ) }}
