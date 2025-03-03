{{ config(
    tags=['init'],
    materialized='ephemeral'
) }}

-- Call the return_vars macro to initialize variables
{% do return_vars() %}

-- Call the log_model_details macro to log model details
{% do log_model_details() %}

-- Return a dummy result
SELECT 
    'Initialization complete' as status,
    CURRENT_TIMESTAMP() as timestamp 