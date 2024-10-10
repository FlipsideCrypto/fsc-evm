{%- if var('GLOBAL_ENABLE_FSC_EVM', False) -%}

{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var('BLOCKS_TRANSACTIONS_REALTIME_NEW_BUILD', False) %}

{% if new_build %}

SELECT  
    0 AS block_number

{% else %}

SELECT
    COALESCE(MIN(block_number), 0) AS block_number
FROM
    {{ ref("silver__blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))

{% endif %}
{%- endif -%}