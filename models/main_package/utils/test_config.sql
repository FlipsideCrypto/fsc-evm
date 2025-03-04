{{
  config(
    materialized = 'table',
    tags = ['testing']
  )
}}

WITH config_test AS (
  SELECT 
    '{{ return_var("GLOBAL_CHAIN_NETWORK") }}' AS chain_network,
    {{ return_var("MAIN_SL_BLOCKS_PER_HOUR") }} AS blocks_per_hour,
    '{{ return_var("GLOBAL_PROD_DB_NAME") }}' AS db_name
)

SELECT * FROM config_test
