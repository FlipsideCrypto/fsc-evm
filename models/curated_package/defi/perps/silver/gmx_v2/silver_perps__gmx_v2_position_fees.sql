{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_perps','defi','perps','curated','gmx','fees']
) }}

{#
GMX v2 Position Fees Model
Captures PositionFeesCollected events from the GMX v2 EventEmitter
This tracks all fees paid by traders including:
- Position fees (trading fees)
- Borrowing fees
- Funding fees
- Liquidation fees (when applicable)

Key event data structure:
- eventData[0][0][0][1] = market
- eventData[0][0][1][1] = collateral_token
- eventData[0][0][3][1] = trader
- eventData[1][0][2][1] = trade_size_usd
- eventData[1][0][9][1] = borrowing_fee_usd
- positionFeeAmount found via LATERAL FLATTEN where value[0] = 'positionFeeAmount'
- liquidationFeeAmount found via LATERAL FLATTEN where value[0] = 'liquidationFeeAmount'
#}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'gmx'
        AND version = 'v2'
),

fee_events AS (
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
        m.protocol,
        m.version,
        CONCAT(m.protocol, '-', m.version) AS platform,
        CONCAT(tx_hash, '-', event_index) AS _log_id,
        modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} l
    INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        decoded_log:eventName::STRING = 'PositionFeesCollected'
        AND tx_succeeded
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
),

-- Extract position fee amount using LATERAL FLATTEN
position_fees_raw AS (
    SELECT
        f.*,
        pos.value[1]::NUMBER AS position_fee_amount_raw
    FROM fee_events f,
    LATERAL FLATTEN(input => f.event_data[1][0]) AS pos
    WHERE pos.value[0]::STRING = 'positionFeeAmount'
),

-- Extract liquidation fee amount (may not exist for all events)
liquidation_fees_raw AS (
    SELECT
        f.tx_hash,
        f.event_index,
        liq.value[1]::NUMBER AS liquidation_fee_amount_raw
    FROM fee_events f,
    LATERAL FLATTEN(input => f.event_data[1][0]) AS liq
    WHERE liq.value[0]::STRING = 'liquidationFeeAmount'
),

parsed_fees AS (
    SELECT
        p.block_number,
        p.block_timestamp,
        p.tx_hash,
        p.origin_function_signature,
        p.origin_from_address,
        p.origin_to_address,
        p.contract_address,
        p.event_index,
        p.event_name,

        -- Address fields
        p.event_data[0][0][0][1]::STRING AS market,
        p.event_data[0][0][1][1]::STRING AS collateral_token,
        p.event_data[0][0][3][1]::STRING AS trader,

        -- Trade size
        TRY_TO_NUMBER(p.event_data[1][0][2][1]::STRING) / 1e30 AS trade_size_usd,

        -- Borrowing fee (already in USD with 1e30 precision)
        TRY_TO_NUMBER(p.event_data[1][0][9][1]::STRING) / 1e30 AS borrowing_fee_usd,

        -- Position fee (in collateral token, needs price conversion)
        p.position_fee_amount_raw,

        -- Trader discount
        COALESCE(TRY_TO_NUMBER(p.event_data[1][0][27][1]::STRING), 0) AS trader_discount_amount_raw,

        -- Liquidation fee
        COALESCE(l.liquidation_fee_amount_raw, 0) AS liquidation_fee_amount_raw,

        -- Order key for joining
        p.event_data[4][0][0][1]::STRING AS order_key,

        p.platform,
        p.protocol,
        p.version,
        p._log_id,
        p.modified_timestamp
    FROM position_fees_raw p
    LEFT JOIN liquidation_fees_raw l
        ON p.tx_hash = l.tx_hash
        AND p.event_index = l.event_index
),

-- Join with prices to get USD values
fees_with_prices AS (
    SELECT
        f.*,
        c.decimals AS collateral_decimals,
        c.symbol AS collateral_symbol,
        p.price AS collateral_price,

        -- Calculate fee amounts
        f.position_fee_amount_raw / POW(10, COALESCE(c.decimals, 18)) AS position_fee_amount,
        (f.position_fee_amount_raw / POW(10, COALESCE(c.decimals, 18))) * COALESCE(p.price, 0) AS position_fee_usd,

        f.trader_discount_amount_raw / POW(10, COALESCE(c.decimals, 18)) AS trader_discount_amount,
        (f.trader_discount_amount_raw / POW(10, COALESCE(c.decimals, 18))) * COALESCE(p.price, 0) AS trader_discount_usd,

        f.liquidation_fee_amount_raw / POW(10, COALESCE(c.decimals, 18)) AS liquidation_fee_amount,
        (f.liquidation_fee_amount_raw / POW(10, COALESCE(c.decimals, 18))) * COALESCE(p.price, 0) AS liquidation_fee_usd

    FROM parsed_fees f
    LEFT JOIN {{ ref('core__dim_contracts') }} c
        ON f.collateral_token = c.address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
        ON f.collateral_token = p.token_address
        AND DATE_TRUNC('hour', f.block_timestamp) = p.hour
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
    trader,
    market,
    collateral_token,
    collateral_symbol,
    collateral_decimals,
    collateral_price,

    -- Trade info
    trade_size_usd,
    order_key,

    -- Fee amounts (native)
    position_fee_amount,
    trader_discount_amount,
    liquidation_fee_amount,
    borrowing_fee_usd,

    -- Fee amounts (USD)
    position_fee_usd,
    trader_discount_usd,
    liquidation_fee_usd,

    -- Net position fee
    position_fee_amount - trader_discount_amount AS net_position_fee_amount,
    position_fee_usd - trader_discount_usd AS net_position_fee_usd,

    -- Total fees
    position_fee_usd + borrowing_fee_usd + liquidation_fee_usd AS total_fees_usd,
    (position_fee_usd - trader_discount_usd) + borrowing_fee_usd + liquidation_fee_usd AS total_fees_net_usd,

    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM fees_with_prices
