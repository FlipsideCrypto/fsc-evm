{%- if var('GLOBAL_ENABLE_FSC_EVM', False) -%}
{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var('TRACES_REALTIME_NEW_BUILD', False) %}

{% if new_build %}

SELECT  
    -1 AS block_number

{% else %}

    WITH lookback AS (
        SELECT
            block_number
        FROM
            {{ ref("_block_lookback") }}
    )
SELECT
    DISTINCT tx.block_number block_number
FROM
    {{ ref("silver__transactions") }}
    tx
    LEFT JOIN {{ ref("silver__traces") }}
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
WHERE
    tx.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.tx_hash IS NULL
    AND tx.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND tr.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.block_timestamp IS NOT NULL

{% endif %}
{%- endif -%}