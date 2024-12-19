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
    {{ ref('bronze__fsc_evm_vars_master_config') }}
