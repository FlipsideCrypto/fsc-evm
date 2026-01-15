{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

with logs as (
    SELECT
        *
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] in (
            '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c',
            '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28',
            '0x7bab3f4000000000000000000000000000000000000000000000000000000000',
            '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
        )
),
bark as(
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN -- Direct modules
            utils.udf_hex_to_int(SUBSTR(data, 67, 64))
            ELSE utils.udf_hex_to_int(SUBSTR(data, 67, 64))::numeric * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x85258d09e1e4ef299ff3fc11e74af99563f022d21f3f940db982229dc2a3358c'
),
grab as(
    SELECT
        block_timestamp,
        tx_hash,
        utils.udf_hex_to_string(rtrim(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7bab3f4000000000000000000000000000000000000000000000000000000000'
),
bite as (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%' THEN -- Direct modules
            utils.udf_hex_to_int(SUBSTR(data, 67, 64))
            ELSE utils.udf_hex_to_int(SUBSTR(data, 67, 64))::numeric * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0xa716da86bc1fb6d43d1493373f34d7a418b619681cd7b90f7ea667ba1489be28'
),
slip_raw as (
    SELECT
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [1] LIKE '%4449524543542%'
            AND length(utils.udf_hex_to_int(topics [3])) < 50 THEN -- Direct modules
            utils.udf_hex_to_int(topics [3])
            WHEN length(utils.udf_hex_to_int(topics [3])) < 50 THEN utils.udf_hex_to_int(topics [3])::numeric * -1
        END AS dart,
        utils.udf_hex_to_string(RTRIM(topics [1], 0)) as ilk
    FROM
        logs
    WHERE
        topics [0] = '0x7cdd3fde00000000000000000000000000000000000000000000000000000000'
),
slip as(
    SELECT
        block_timestamp,
        tx_hash,
        min(dart) as dart,
        -- collision on certain tx where there are two calls to slip()
        ilk
    FROM
        slip_raw
    GROUP BY
        1,
        2,
        4
),
agg as(
    SELECT
        distinct g.block_timestamp,
        g.tx_hash as tx_hash,
        coalesce(b.dart, t.dart, s.dart) as dart,
        g.ilk as ilk
    FROM
        grab g
        LEFT JOIN bark b on b.tx_hash = g.tx_hash
        and g.ilk = b.ilk
        LEFT JOIN bite t on t.tx_hash = g.tx_hash
        and g.ilk = t.ilk
        LEFT JOIN slip s on s.tx_hash = g.tx_hash
        and g.ilk = s.ilk
)
select
    *
from
    agg
where
    dart is not null