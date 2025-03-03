{{ config(
    tags=['init'],
    materialized='ephemeral'
) }}

-- Set default values for required variables if they're not already set
{% if var('MAIN_SL_BLOCKS_PER_HOUR', 0) == 0 %}
    {% do log('WARNING: MAIN_SL_BLOCKS_PER_HOUR is set to 0. This may cause issues in calculations.', info=true) %}
{% endif %}

{% if var('MAIN_SL_TRANSACTIONS_PER_BLOCK', 0) == 0 %}
    {% do log('WARNING: MAIN_SL_TRANSACTIONS_PER_BLOCK is set to 0. This may cause issues in calculations.', info=true) %}
{% endif %}

-- Call the return_vars macro to initialize variables
{% do return_vars() %}

-- Call the log_model_details macro to log model details
{% do log_model_details() %}

-- Return a dummy result
SELECT 
    'Initialization complete' as status,
    CURRENT_TIMESTAMP() as timestamp 