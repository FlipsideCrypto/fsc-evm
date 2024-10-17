{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var('BLOCKS_TRANSACTIONS_REALTIME_NEW_BUILD', false) %}

{% if new_build %}

SELECT  
    0 AS block_number

{% else %}

SELECT
    COALESCE(MIN(block_number), 0) AS block_number
FROM
    {{ ref("core__fact_blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))

{% endif %}