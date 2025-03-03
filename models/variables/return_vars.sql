{{ config(
    tags=['variables_init'],
    materialized='table'
) }}

{% do load_all_variables() %}

-- Return a dummy table with the initialization status
SELECT 
    'Variables initialized' as status,
    CURRENT_TIMESTAMP() as init_time 