{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','lp_actions','curated']
) }}

WITH evt AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        p.pool_address,
        decoded_log :provider :: STRING AS liquidity_provider,
        COALESCE(decoded_log :token_amounts :: STRING, decoded_log :token_amount :: STRING) AS token_amounts,
        token_amounts [0] :: STRING AS amount0_unadj,
        token_amounts [1] :: STRING AS amount1_unadj,
        token_amounts [2] :: STRING AS amount2_unadj,
        token_amounts [3] :: STRING AS amount3_unadj,
        token_amounts [4] :: STRING AS amount4_unadj,
        token_amounts [5] :: STRING AS amount5_unadj,
        token_amounts [6] :: STRING AS amount6_unadj,
        token_amounts [7] :: STRING AS amount7_unadj,
        COALESCE(decoded_log :fees :: STRING, decoded_log :fee :: STRING) AS fees,
        fees [0] :: STRING AS fee0,
        fees [1] :: STRING AS fee1,
        fees [2] :: STRING AS fee2,
        fees [3] :: STRING AS fee3,
        fees [4] :: STRING AS fee4,
        fees [5] :: STRING AS fee5,
        fees [6] :: STRING AS fee6,
        fees [7] :: STRING AS fee7,
        decoded_log :invariant :: STRING AS invariant,
        decoded_log: token_supply :: STRING AS token_supply,
        decoded_log: packed_price_scale :: STRING AS packed_price_scale,
        p.protocol,
        p.version,
        p.type,
        CONCAT(
            p.protocol,
            '-',
            p.version
        ) AS platform,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
        l
        INNER JOIN {{ref('silver_dex__curve_pools')}} p
        ON l.contract_address = p.pool_address
    WHERE
        topic_0 IN (
            '0xe1b60455bd9e33720b547f60e4e0cfbf1252d0f2ee0147d53029945f39fe3c1a',
            '0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d',
            '0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768',
            '0x96b486485420b963edd3fdec0b0195730035600feb7de6f544383d7950fa97ee',
            '0x540ab385f9b5d450a27404172caade516b3ba3f4be88239ac56a2ad1de2a1f5a',
            '0x3f1915775e0c9a38a57a7bb7f1f9005f486fb904e1f84aa215364d567319a58d', -- AddLiquidity
            '0x9878ca375e106f2a43c3b599fc624568131c4c9a4ba66a14563715763be9d59d',
            '0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c',
            '0xd6cc314a0b1e3b2579f8e64248e82434072e8271290eef8ad0886709304195f5',
            '0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d',
            '0xdd3c0336a16f1b64f172b7bb0dad5b2b3c7c76f91e8c4aafd6aae60dce800153' -- RemoveLiquidity
        )
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
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        amount_precise,
        amount
    FROM 
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                evt
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
pool_tokens AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        pool_address,
        liquidity_provider,
        token_amounts,
        t0.contract_address AS token0,
        t1.contract_address AS token1,
        t2.contract_address AS token2,
        t3.contract_address AS token3,
        t4.contract_address AS token4,
        t5.contract_address AS token5,
        t6.contract_address AS token6,
        t7.contract_address AS token7,
        amount0_unadj,
        amount1_unadj,
        amount2_unadj,
        amount3_unadj,
        amount4_unadj,
        amount5_unadj,
        amount6_unadj,
        amount7_unadj,
        fees,
        fee0,
        fee1,
        fee2,
        fee3,
        fee4,
        fee5,
        fee6,
        fee7,
        invariant,
        token_supply,
        packed_price_scale,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM evt e 
    LEFT JOIN transfers t0
    ON e.block_number = t0.block_number
    AND e.tx_hash = t0.tx_hash
    AND e.amount0_unadj = t0.amount
    LEFT JOIN transfers t1
    ON e.block_number = t1.block_number
    AND e.tx_hash = t1.tx_hash
    AND e.amount1_unadj = t1.amount
    LEFT JOIN transfers t2
    ON e.block_number = t2.block_number
    AND e.tx_hash = t2.tx_hash
    AND e.amount2_unadj = t2.amount
    LEFT JOIN transfers t3
    ON e.block_number = t3.block_number
    AND e.tx_hash = t3.tx_hash
    AND e.amount3_unadj = t3.amount
    LEFT JOIN transfers t4
    ON e.block_number = t4.block_number
    AND e.tx_hash = t4.tx_hash
    AND e.amount4_unadj = t4.amount
    LEFT JOIN transfers t5
    ON e.block_number = t5.block_number
    AND e.tx_hash = t5.tx_hash
    AND e.amount5_unadj = t5.amount
    LEFT JOIN transfers t6
    ON e.block_number = t6.block_number
    AND e.tx_hash = t6.tx_hash
    AND e.amount6_unadj = t6.amount
    LEFT JOIN transfers t7
    ON e.block_number = t7.block_number
    AND e.tx_hash = t7.tx_hash
    AND e.amount7_unadj = t7.amount
),
add_liquidity AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'AddLiquidity' AS event_name,
        contract_address,
        pool_address,
        token_amounts,
        fees,
        invariant,
        token_supply,
        packed_price_scale,
        liquidity_provider,
        liquidity_provider AS sender,
        pool_address AS receiver,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        amount0_unadj,
        amount1_unadj,
        amount2_unadj,
        amount3_unadj,
        amount4_unadj,
        amount5_unadj,
        amount6_unadj,
        amount7_unadj,
        fee0,
        fee1,
        fee2,
        fee3,
        fee4,
        fee5,
        fee6,
        fee7,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        pool_tokens
    WHERE topic_0 IN (
            '0xe1b60455bd9e33720b547f60e4e0cfbf1252d0f2ee0147d53029945f39fe3c1a',
            '0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d',
            '0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768',
            '0x96b486485420b963edd3fdec0b0195730035600feb7de6f544383d7950fa97ee',
            '0x540ab385f9b5d450a27404172caade516b3ba3f4be88239ac56a2ad1de2a1f5a',
            '0x3f1915775e0c9a38a57a7bb7f1f9005f486fb904e1f84aa215364d567319a58d' -- AddLiquidity
        )
),
remove_liquidity AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'RemoveLiquidity' AS event_name,
        contract_address,
        pool_address,
        token_amounts,
        fees,
        invariant,
        token_supply,
        packed_price_scale,
        liquidity_provider,
        liquidity_provider AS sender,
        pool_address AS receiver,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        amount0_unadj,
        amount1_unadj,
        amount2_unadj,
        amount3_unadj,
        amount4_unadj,
        amount5_unadj,
        amount6_unadj,
        amount7_unadj,
        fee0,
        fee1,
        fee2,
        fee3,
        fee4,
        fee5,
        fee6,
        fee7,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        pool_tokens
    WHERE topic_0 IN (
            '0x9878ca375e106f2a43c3b599fc624568131c4c9a4ba66a14563715763be9d59d',
            '0x7c363854ccf79623411f8995b362bce5eddff18c927edc6f5dbbb5e05819a82c',
            '0xd6cc314a0b1e3b2579f8e64248e82434072e8271290eef8ad0886709304195f5',
            '0xa49d4cf02656aebf8c771f5a8585638a2a15ee6c97cf7205d4208ed7c1df252d',
            '0xdd3c0336a16f1b64f172b7bb0dad5b2b3c7c76f91e8c4aafd6aae60dce800153' -- RemoveLiquidity
        )
),
all_actions AS (
    SELECT
        *
    FROM
        add_liquidity
    UNION ALL
    SELECT
        *
    FROM
        remove_liquidity
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_name,
    contract_address,
    pool_address,
    token_amounts,
    fees,
    invariant,
    token_supply,
    packed_price_scale,
    liquidity_provider,
    sender,
    receiver,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    amount0_unadj,
    amount1_unadj,
    amount2_unadj,
    amount3_unadj,
    amount4_unadj,
    amount5_unadj,
    amount6_unadj,
    amount7_unadj,
    fee0,
    fee1,
    fee2,
    fee3,
    fee4,
    fee5,
    fee6,
    fee7,
    protocol,
    version,
    type,
    platform,
    _log_id,
    modified_timestamp
FROM
    all_actions qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
