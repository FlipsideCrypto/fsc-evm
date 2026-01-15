{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_perps','defi','perps','curated','gmx']
) }}

{#
GMX v2 Position Changes Model
Captures PositionIncrease and PositionDecrease events from the GMX v2 EventEmitter
EventEmitter contract addresses:
- Arbitrum: 0xc8ee91a54287db53897056e12d9819156d3822fb
- Avalanche: 0xdb17b211c34240b014ab6d61d4a31fa0c0e20c26

Key event data structure for PositionIncrease/Decrease:
- eventData[0][0][0][1] = market
- eventData[0][0][1][1] = collateral_token
- eventData[0][0][3][1] = account (trader)
- eventData[1][0][2][1] = size_in_usd (div by 1e30)
- eventData[1][0][3][1] = size_in_tokens
- eventData[1][0][4][1] = collateral_amount
- eventData[1][0][7][1] = execution_price
- eventData[1][0][16][1] = order_type (7 = liquidation)
- eventData[3][0][0][1] = is_long
- eventData[4][0][0][1] = order_key
#}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'gmx'
        AND version = 'v2'
),

position_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        event_index,
        decoded_log,
        decoded_log:eventName::STRING AS event_name,
        decoded_log:eventData AS event_data,
        decoded_log:msgSender::STRING AS msg_sender,
        m.protocol,
        m.version,
        CONCAT(m.protocol, '-', m.version) AS platform,
        CONCAT(tx_hash, '-', event_index) AS _log_id,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} l
    INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        decoded_log:eventName::STRING IN ('PositionIncrease', 'PositionDecrease')
        AND tx_succeeded
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
),

parsed_positions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        msg_sender,

        -- Address fields (eventData[0] = addressItems)
        event_data[0][0][0][1]::STRING AS market,
        event_data[0][0][1][1]::STRING AS collateral_token,
        event_data[0][0][3][1]::STRING AS trader,

        -- Size and collateral (eventData[1] = uintItems)
        -- Index 0: sizeInUsd (precision 1e30)
        TRY_TO_NUMBER(event_data[1][0][0][1]::STRING) / 1e30 AS size_usd,
        -- Index 1: sizeInTokens
        TRY_TO_NUMBER(event_data[1][0][1][1]::STRING) AS size_in_tokens_raw,
        -- Index 2: collateralAmount
        TRY_TO_NUMBER(event_data[1][0][2][1]::STRING) AS collateral_amount_raw,
        -- Index 12: sizeDeltaUsd (precision 1e30)
        TRY_TO_NUMBER(event_data[1][0][12][1]::STRING) / 1e30 AS size_delta_usd,
        -- Index 13: sizeDeltaInTokens
        TRY_TO_NUMBER(event_data[1][0][13][1]::STRING) AS size_delta_tokens_raw,
        -- Index 15: collateralDeltaAmount
        TRY_TO_NUMBER(event_data[1][0][15][1]::STRING) AS collateral_delta_raw,

        -- Prices (eventData[1] = uintItems)
        -- Index 7: executionPrice (precision varies by token, typically 1e12 for ETH/BTC)
        TRY_TO_NUMBER(event_data[1][0][7][1]::STRING) / 1e12 AS execution_price,
        -- Index 8: indexTokenPrice.max
        TRY_TO_NUMBER(event_data[1][0][8][1]::STRING) / 1e12 AS index_token_price_max,
        -- Index 9: indexTokenPrice.min
        TRY_TO_NUMBER(event_data[1][0][9][1]::STRING) / 1e12 AS index_token_price_min,

        -- Order info
        -- Index 14: orderType (0=MarketSwap, 2=MarketIncrease, 4=MarketDecrease, 7=Liquidation)
        TRY_TO_NUMBER(event_data[1][0][14][1]::STRING) AS order_type,
        -- eventData[4] = bytes32Items
        event_data[4][0][0][1]::STRING AS order_key,
        event_data[4][0][1][1]::STRING AS position_key,

        -- Direction (eventData[3] = boolItems)
        event_data[3][0][0][1]::BOOLEAN AS is_long,

        -- Impact (eventData[2] = intItems - can be negative)
        TRY_TO_NUMBER(event_data[2][0][0][1]::STRING) / 1e30 AS price_impact_usd,

        platform,
        protocol,
        version,
        _log_id,
        modified_timestamp
    FROM position_events
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    msg_sender,
    trader,
    market,
    collateral_token,

    -- Trade direction and type
    CASE WHEN is_long THEN 'long' ELSE 'short' END AS trade_direction,
    CASE
        WHEN event_name = 'PositionIncrease' THEN 'open'
        WHEN event_name = 'PositionDecrease' AND order_type = 7 THEN 'liquidation'
        WHEN event_name = 'PositionDecrease' THEN 'close'
    END AS trade_type,
    is_long,

    -- Sizes
    size_usd,
    size_delta_usd AS volume_usd,
    size_in_tokens_raw,
    size_delta_tokens_raw,
    collateral_amount_raw,
    collateral_delta_raw,

    -- Prices
    execution_price,
    index_token_price_max,
    index_token_price_min,
    price_impact_usd,

    -- Order info
    order_type,
    order_key,
    position_key,

    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM parsed_positions
