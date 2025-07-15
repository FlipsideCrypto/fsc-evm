{# Get Variables #}
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

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'gmx'
        AND version = 'v2'
),
decoded_logs AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        topics,
        DATA,
        event_index,
        decoded_log,
        decoded_log :eventName :: STRING AS event_name,
        decoded_log :eventNameHash :: STRING AS event_name_hash,
        decoded_log :msgSender :: STRING AS msg_sender,
        decoded_log :topic1 :: STRING AS topic_1,
        decoded_log :topic2 :: STRING AS topic_2,
        decoded_log :eventData AS event_data,
        m.protocol,
        m.version,
        CONCAT(m.protocol, '-', m.version) AS platform,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }} 
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        decoded_log :eventName :: STRING IN (
            'SwapInfo',
            'OrderExecuted'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
parse_data AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        modified_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        event_data [0] [0] [0] [1] :: STRING AS market,
        event_data [0] [0] [1] [1] :: STRING AS receiver,
        event_data [0] [0] [2] [1] :: STRING AS token_in,
        event_data [0] [0] [3] [1] :: STRING AS token_out,
        TRY_TO_NUMBER(
            event_data [1] [0] [0] [1] :: STRING
        ) AS token_in_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [1] [1] :: STRING
        ) AS token_out_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [2] [1] :: STRING
        ) AS amount_in,
        TRY_TO_NUMBER(
            event_data [1] [0] [3] [1] :: STRING
        ) AS amount_in_after_fees,
        TRY_TO_NUMBER(
            event_data [1] [0] [4] [1] :: STRING
        ) AS amount_out,
        TRY_TO_NUMBER(
            event_data [2] [0] [0] [1] :: STRING
        ) AS price_impact_usd,
        TRY_TO_NUMBER(
            event_data [2] [0] [0] [1] :: STRING
        ) AS price_impact_amount,
        event_data [4] [0] [0] [1] :: STRING AS key
    FROM
        decoded_logs
    WHERE
        event_name = 'SwapInfo'
),
column_format AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        p.contract_address,
        event_index,
        _log_id,
        modified_timestamp,
        event_name,
        event_name_hash,
        msg_sender AS sender,
        receiver AS tx_to,
        topic_1,
        market,
        receiver,
        token_in,
        token_in_price AS raw_token_in_price,
        amount_in AS amount_in_unadj,
        amount_in_after_fees,
        token_out,
        token_out_price AS raw_token_out_price,
        amount_out AS amount_out_unadj,
        price_impact_usd,
        price_impact_amount,
        key,
        platform,
        protocol,
        version
    FROM
        parse_data p
),
executed_orders AS (
    SELECT
        event_data [4] [0] [0] [1] :: STRING AS key
    FROM
        decoded_logs
    WHERE
        event_name = 'OrderExecuted'
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.origin_function_signature,
    A.origin_from_address,
    A.origin_to_address,
    A.contract_address,
    A.event_index,
    A.event_name,
    market,
    receiver,
    sender,
    tx_to,
    CASE
        WHEN e.key IS NOT NULL THEN 'executed'
        ELSE 'not-executed'
    END AS order_execution,
    token_in,
    amount_in_unadj,
    token_out,
    amount_out_unadj,
    platform,
    protocol,
    version,
    A.key,
    A._log_id,
    A.modified_timestamp
FROM
    column_format A
    INNER JOIN executed_orders e
    ON A.key = e.key
