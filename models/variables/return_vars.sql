{{ config(
    tags=['return_vars'],
    materialized='table'
) }}

{% do return_vars() %}

-- Return a dummy table with the initialization status
SELECT 
    'Variables initialized' as status,
    CURRENT_TIMESTAMP() as init_time 