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
        tokens [0] :: STRING AS token_0,
        tokens [1] :: STRING AS token_1,
        tokens [2] :: STRING AS token_2,
        tokens [3] :: STRING AS token_3,
        tokens [4] :: STRING AS token_4,
        tokens [5] :: STRING AS token_5,
        tokens [6] :: STRING AS token_6,
        tokens [7] :: STRING AS token_7,
        decoded_log :deltas AS deltas,
        TRY_TO_NUMBER(deltas [0] :: STRING) AS amount_0,
        TRY_TO_NUMBER(deltas [1] :: STRING) AS amount_1,
        TRY_TO_NUMBER(deltas [2] :: STRING) AS amount_2,
        TRY_TO_NUMBER(deltas [3] :: STRING) AS amount_3,
        TRY_TO_NUMBER(deltas [4] :: STRING) AS amount_4,
        TRY_TO_NUMBER(deltas [5] :: STRING) AS amount_5,
        TRY_TO_NUMBER(deltas [6] :: STRING) AS amount_6,
        TRY_TO_NUMBER(deltas [7] :: STRING) AS amount_7,
        decoded_log :protocolFeeAmounts AS protocol_fee_amounts,
        TRY_TO_NUMBER(protocol_fee_amounts [0] :: STRING) AS protocol_fee_amount_0,
        TRY_TO_NUMBER(protocol_fee_amounts [1] :: STRING) AS protocol_fee_amount_1,
        TRY_TO_NUMBER(protocol_fee_amounts [2] :: STRING) AS protocol_fee_amount_2,
        TRY_TO_NUMBER(protocol_fee_amounts [3] :: STRING) AS protocol_fee_amount_3,
        TRY_TO_NUMBER(protocol_fee_amounts [4] :: STRING) AS protocol_fee_amount_4,
        TRY_TO_NUMBER(protocol_fee_amounts [5] :: STRING) AS protocol_fee_amount_5,
        TRY_TO_NUMBER(protocol_fee_amounts [6] :: STRING) AS protocol_fee_amount_6,
        TRY_TO_NUMBER(protocol_fee_amounts [7] :: STRING) AS protocol_fee_amount_7,
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
        token_0,
        token_1,
        token_2,
        token_3,
        token_4,
        token_5,
        token_6,
        token_7,
        liquidity_provider,
        liquidity_provider AS sender,
        pool_address AS receiver,
        amount_0 AS amount_0_unadj,
        amount_1 AS amount_1_unadj,
        amount_2 AS amount_2_unadj,
        amount_3 AS amount_3_unadj,
        amount_4 AS amount_4_unadj,
        amount_5 AS amount_5_unadj,
        amount_6 AS amount_6_unadj,
        amount_7 AS amount_7_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        amount_0 >= 0
        OR amount_1 >= 0
        OR amount_2 >= 0
        OR amount_3 >= 0
        OR amount_4 >= 0
        OR amount_5 >= 0
        OR amount_6 >= 0
        OR amount_7 >= 0
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
        token_0,
        token_1,
        token_2,
        token_3,
        token_4,
        token_5,
        token_6,
        token_7,
        liquidity_provider,
        pool_address AS sender,
        liquidity_provider AS receiver,
        amount_0 AS amount_0_unadj,
        amount_1 AS amount_1_unadj,
        amount_2 AS amount_2_unadj,
        amount_3 AS amount_3_unadj,
        amount_4 AS amount_4_unadj,
        amount_5 AS amount_5_unadj,
        amount_6 AS amount_6_unadj,
        amount_7 AS amount_7_unadj,
        protocol,
        version,
        type,
        platform,
        _log_id,
        modified_timestamp
    FROM
        evt
    WHERE
        amount_0 < 0
        OR amount_1 < 0
        OR amount_2 < 0
        OR amount_3 < 0
        OR amount_4 < 0
        OR amount_5 < 0
        OR amount_6 < 0
        OR amount_7 < 0
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
    token_0,
    token_1,
    token_2,
    token_3,
    token_4,
    token_5,
    token_6,
    token_7,
    liquidity_provider,
    sender,
    receiver,
    amount_0_unadj,
    amount_1_unadj,
    amount_2_unadj,
    amount_3_unadj,
    amount_4_unadj,
    amount_5_unadj,
    amount_6_unadj,
    amount_7_unadj,
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
