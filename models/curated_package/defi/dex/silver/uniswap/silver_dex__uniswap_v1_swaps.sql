{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH swaps AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS buyer_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: FLOAT AS sold_amount,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: FLOAT AS bought_amount,
        p.token0,
        p.token1,
        p.platform,
        p.protocol,
        p.version,
        p.type,
        CASE
            WHEN topics [0] :: STRING = '0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f' THEN 'TokenPurchase'
            ELSE 'EthPurchase'
        END AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__uniswap_v1_pools') }}
        p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING IN (
            '0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f',
            --TokenPurchase
            '0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705'
        ) --EthPurchase
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: FLOAT AS amount
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                swaps
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
eth_purchase AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        s.contract_address,
        buyer_address AS sender,
        to_address AS tx_to,
        sold_amount AS amount_in_unadj,
        bought_amount AS amount_out_unadj,
        t.contract_address AS token_in,
        '0x0000000000000000000000000000000000000000' AS token_out,
        --native ETH, for pricing purposes
        platform,
        protocol,
        version,
        TYPE,
        event_name,
        _log_id,
        modified_timestamp
    FROM
        swaps s
        INNER JOIN transfers t
        ON s.tx_hash = t.tx_hash
        AND s.buyer_address = t.from_address
        AND s.sold_amount = t.amount
        AND s.event_name = 'EthPurchase'
),
token_purchase AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        s.contract_address,
        buyer_address AS sender,
        to_address AS tx_to,
        sold_amount AS amount_in_unadj,
        bought_amount AS amount_out_unadj,
        '0x0000000000000000000000000000000000000000' AS token_in,
        t.contract_address AS token_out,
        platform,
        protocol,
        version,
        TYPE,
        event_name,
        _log_id,
        modified_timestamp
    FROM
        swaps s
        INNER JOIN transfers t
        ON s.tx_hash = t.tx_hash
        AND s.buyer_address = t.to_address
        AND s.bought_amount = t.amount
        AND s.event_name = 'TokenPurchase'
),
all_swaps AS (
    SELECT
        *
    FROM
        eth_purchase
    UNION ALL
    SELECT
        *
    FROM
        token_purchase
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    sender,
    tx_to,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    platform,
    protocol,
    version,
    TYPE,
    event_name,
    _log_id,
    modified_timestamp
FROM
    all_swaps
