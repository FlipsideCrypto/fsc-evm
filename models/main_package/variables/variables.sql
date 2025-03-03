{{ config(
    materialized='table',
    tags=['variables']
) }}

-- Return a simple table with the variable values
SELECT 
    'MAIN_SL_BLOCKS_PER_HOUR' as variable_name,
    {{ var('MAIN_SL_BLOCKS_PER_HOUR', 240) }} as variable_value
UNION ALL
SELECT 
    'MAIN_SL_TRANSACTIONS_PER_BLOCK' as variable_name,
    {{ var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 100) }} as variable_value
UNION ALL
SELECT 
    'MAIN_CORE_RECEIPTS_BY_HASH_ENABLED' as variable_name,
    {{ var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) }} as variable_value 