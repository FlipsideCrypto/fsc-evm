{% set vars = return_vars() %}
{{
  config(
    materialized = 'table',
    tags = ['testing']
  )
}}

WITH config_test AS (
  SELECT 
    {{ vars.GLOBAL_CHAIN_NETWORK }} AS chain_network,
    {{ vars.MAIN_SL_BLOCKS_PER_HOUR }} AS blocks_per_hour,
    {{ vars.GLOBAL_PROD_DB_NAME }} AS db_name
)

SELECT * FROM config_test