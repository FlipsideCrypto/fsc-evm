{{ config(
    materialized = 'table'
) }}

SELECT
    *
FROM
    {{ ref("core__fact_blocks") }}
LIMIT
    1
