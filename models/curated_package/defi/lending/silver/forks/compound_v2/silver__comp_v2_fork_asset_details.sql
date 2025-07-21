{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH contracts AS (

    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
log_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        l.modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND C.token_name LIKE '%Comp V2 Fork%'

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.contract_address NOT IN (
    SELECT
        token_address
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
traces_pull AS (
    SELECT
        t.from_address AS token_address,
        t.to_address AS underlying_asset,
        CASE
            WHEN TYPE = 'STATICCALL'
            AND trace_address = '0_2' THEN 1
            ELSE NULL
        END AS asset_identifier
    FROM
        {{ ref('core__fact_traces') }}
        t
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
),
underlying_details AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        l.token_name,
        l.token_symbol,
        l.token_decimals,
        t.underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
    WHERE
        t.asset_identifier = 1 qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    underlying_details l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    l.token_name IS NOT NULL
