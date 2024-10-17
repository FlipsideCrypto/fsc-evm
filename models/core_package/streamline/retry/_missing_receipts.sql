{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var('RECEIPTS_REALTIME_NEW_BUILD', false) %}
{% set new_build_by_hash = var('RECEIPTS_BY_HASH_REALTIME_NEW_BUILD', false) %}

{% if new_build or new_build_by_hash %}

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
    DISTINCT t.block_number AS block_number
FROM
    {{ ref("core__fact_transactions") }}
    t
    LEFT JOIN {{ ref("silver__receipts") }}
    r USING (
        block_number,
        tx_hash
    )
WHERE
    r.tx_hash IS NULL
    AND t.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND t.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND (
        r._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR r._inserted_timestamp IS NULL)

{% endif %}