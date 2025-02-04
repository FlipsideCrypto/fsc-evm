{{ config(
    materialized = 'view'
) }}

SELECT
    var_id,
    chain,
    VALUE,
    is_enabled
FROM
    {{ source(
        'fsc_evm_bronze',
        'fsc_evm_vars_chain_values'
    ) }}
