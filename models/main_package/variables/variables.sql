{{ config(
    materialized='ephemeral',
    tags=['variables']
) }}

-- Call the return_vars macro to initialize variables
{% do return_vars() %}

-- Return a dummy result
SELECT 
    'Variables initialized' as status,
    CURRENT_TIMESTAMP() as timestamp 