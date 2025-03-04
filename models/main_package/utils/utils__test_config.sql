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
    '{{ return_var("GLOBAL_PROD_DB_NAME") }}' AS db_name,
    {{ return_var("CHAINHEAD_SQL_LIMIT") }} AS chainhead_sql_limit,
    '{{ return_var("VERTEX_CONTRACTS.ABI") }}' AS vertex_abi,
    '{{ return_var("VERTEX_CONTRACTS.ADDRESS") }}' AS vertex_address
)

SELECT * FROM config_test