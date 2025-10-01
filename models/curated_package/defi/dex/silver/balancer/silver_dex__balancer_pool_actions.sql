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
        decoded_log :poolId :: STRING AS pool_id,
        decoded_log :liquidityProvider :: STRING AS liquidity_provider,
        decoded_log :tokens AS tokens,
        tokens [0] :: STRING AS token0,
        tokens [1] :: STRING AS token1,
        tokens [2] :: STRING AS token2,
        tokens [3] :: STRING AS token3,
        tokens [4] :: STRING AS token4,
        tokens [5] :: STRING AS token5,
        tokens [6] :: STRING AS token6,
        tokens [7] :: STRING AS token7,
        decoded_log :deltas AS deltas,
        TRY_TO_NUMBER(deltas [0] :: STRING) AS amount0,
        TRY_TO_NUMBER(deltas [1] :: STRING) AS amount1,
        TRY_TO_NUMBER(deltas [2] :: STRING) AS amount2,
        TRY_TO_NUMBER(deltas [3] :: STRING) AS amount3,
        TRY_TO_NUMBER(deltas [4] :: STRING) AS amount4,
        TRY_TO_NUMBER(deltas [5] :: STRING) AS amount5,
        TRY_TO_NUMBER(deltas [6] :: STRING) AS amount6,
        TRY_TO_NUMBER(deltas [7] :: STRING) AS amount7,
        decoded_log :protocolFeeAmounts AS protocol_fee_amounts,
        TRY_TO_NUMBER(protocol_fee_amounts [0] :: STRING) AS protocol_fee_amount0,
        TRY_TO_NUMBER(protocol_fee_amounts [1] :: STRING) AS protocol_fee_amount1,
        TRY_TO_NUMBER(protocol_fee_amounts [2] :: STRING) AS protocol_fee_amount2,
        TRY_TO_NUMBER(protocol_fee_amounts [3] :: STRING) AS protocol_fee_amount3,
        TRY_TO_NUMBER(protocol_fee_amounts [4] :: STRING) AS protocol_fee_amount4,
        TRY_TO_NUMBER(protocol_fee_amounts [5] :: STRING) AS protocol_fee_amount5,
        TRY_TO_NUMBER(protocol_fee_amounts [6] :: STRING) AS protocol_fee_amount6,
        TRY_TO_NUMBER(protocol_fee_amounts [7] :: STRING) AS protocol_fee_amount7,
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
        INNER JOIN {{ref('silver_dex__balancer_pools')}} p
        ON l.contract_address = p.contract_address
    WHERE
        topic_0 = '0xe5ce249087ce04f05a957192435400fd97868dba0e6a4b4c049abf8af80dae78' --PoolBalanceChanged
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
deposit AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'Deposit' AS event_name,
        contract_address,
        pool_address,
        pool_id,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        liquidity_provider,
        liquidity_provider AS sender,
        pool_address AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        amount2 AS amount2_unadj,
        amount3 AS amount3_unadj,
        amount4 AS amount4_unadj,
        amount5 AS amount5_unadj,
        amount6 AS amount6_unadj,
        amount7 AS amount7_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        amount0 >= 0
        OR amount1 >= 0
        OR amount2 >= 0
        OR amount3 >= 0
        OR amount4 >= 0
        OR amount5 >= 0
        OR amount6 >= 0
        OR amount7 >= 0
),
withdraw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'Withdraw' AS event_name,
        contract_address,
        pool_address,
        pool_id,
        token0,
        token1,
        token2,
        token3,
        token4,
        token5,
        token6,
        token7,
        liquidity_provider,
        pool_address AS sender,
        liquidity_provider AS receiver,
        amount0 AS amount0_unadj,
        amount1 AS amount1_unadj,
        amount2 AS amount2_unadj,
        amount3 AS amount3_unadj,
        amount4 AS amount4_unadj,
        amount5 AS amount5_unadj,
        amount6 AS amount6_unadj,
        amount7 AS amount7_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        amount0 < 0
        OR amount1 < 0
        OR amount2 < 0
        OR amount3 < 0
        OR amount4 < 0
        OR amount5 < 0
        OR amount6 < 0
        OR amount7 < 0
),
all_actions AS (
    SELECT
        *
    FROM
        deposit
    UNION ALL
    SELECT
        *
    FROM
        withdraw
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
    pool_id,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    liquidity_provider,
    sender,
    receiver,
    amount0_unadj,
    amount1_unadj,
    amount2_unadj,
    amount3_unadj,
    amount4_unadj,
    amount5_unadj,
    amount6_unadj,
    amount7_unadj,
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
