{{ config(
    materialized='table',
    tags=['maintenance', 'search_optimization'],
    enabled=true
) }}

{{ run_daily_search_optimization() }} 