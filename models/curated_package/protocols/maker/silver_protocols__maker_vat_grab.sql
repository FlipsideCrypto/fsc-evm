{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash', 'ilk'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'vat_grab', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH logs AS (
    SELECT
        *
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] IN (
            '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c',
            '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28',
            '0x7bab3f4000000000000000000000000000000000000000000000000000000000',
            '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
        )
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
),

bark AS (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN
            utils.udf_hex_to_int(SUBSTR(data, 67, 64))
            ELSE utils.udf_hex_to_int(SUBSTR(data, 67, 64))::NUMERIC * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) AS ilk
    FROM
        logs
    WHERE
        topics [0] = '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c'
),

grab AS (
    SELECT
        block_timestamp,
        tx_hash,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) AS ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7bab3f4000000000000000000000000000000000000000000000000000000000'
),

bite AS (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN
            utils.udf_hex_to_int(SUBSTR(data, 67, 64))
            ELSE utils.udf_hex_to_int(SUBSTR(data, 67, 64))::NUMERIC * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) AS ilk
    FROM
        logs
    WHERE
        topics [0] = '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28'
),

slip_raw AS (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%'
            AND LENGTH(utils.udf_hex_to_int(topics [3])) < 50 THEN
            utils.udf_hex_to_int(topics [3])
            WHEN LENGTH(utils.udf_hex_to_int(topics [3])) < 50 THEN utils.udf_hex_to_int(topics [3])::NUMERIC * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) AS ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
),

slip AS (
    SELECT
        block_timestamp,
        tx_hash,
        MIN(dart) AS dart,
        ilk
    FROM
        slip_raw
    GROUP BY
        1, 2, 4
),

agg AS (
    SELECT DISTINCT
        g.block_timestamp,
        g.tx_hash AS tx_hash,
        COALESCE(b.dart, t.dart, s.dart) AS dart,
        g.ilk AS ilk
    FROM
        grab g
        LEFT JOIN bark b ON b.tx_hash = g.tx_hash
        AND g.ilk = b.ilk
        LEFT JOIN bite t ON t.tx_hash = g.tx_hash
        AND g.ilk = t.ilk
        LEFT JOIN slip s ON s.tx_hash = g.tx_hash
        AND g.ilk = s.ilk
)

SELECT
    block_timestamp,
    tx_hash,
    dart,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    agg
WHERE
    dart IS NOT NULL
