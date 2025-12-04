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
        CASE
            WHEN topics [0] :: STRING = '0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705' THEN 'EthPurchase'
            ELSE 'TokenPurchase'
        END AS event_name,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS buyer_address,
        utils.udf_hex_to_int(
            topics [2] :: STRING
        ) :: FLOAT AS sold_amount,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: FLOAT AS bought_amount,
        CASE
            WHEN topics [0] :: STRING = '0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705' THEN p.token0
            ELSE p.token1
        END AS token_in,
        CASE
            WHEN topics [0] :: STRING = '0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705' THEN p.token1
            ELSE p.token0
        END AS token_out,
        p.platform,
        p.protocol,
        p.version,
        p.type,
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
    buyer_address AS sender,
    sender AS tx_to,
    sold_amount AS amount_in_unadj,
    bought_amount AS amount_out_unadj,
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
    swaps
