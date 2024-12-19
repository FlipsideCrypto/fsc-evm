{{ config(
    materialized = 'view'
) }}

SELECT
    var_id,
    chain,
    VALUE,
    is_enabled
FROM
    {{ ref('bronze__fsc_evm_vars_chain_values') }}
