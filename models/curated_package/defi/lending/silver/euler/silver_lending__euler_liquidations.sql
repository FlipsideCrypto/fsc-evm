{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','euler','liquidations']
) }}

WITH token_meta AS (
    SELECT
        contract_address,
        token_name,
        token_symbol,
        token_decimals,
        segmented_data,
        creator,
        underlying_address,
        underlying_name,
        protocol,
        version,
        dToken
    FROM
        {{ ref('silver_lending__euler_tokens') }}
),
base AS(
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS debt_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS liquidator,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS collateral_asset,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS repaid_amount_unadj,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS yield_balance,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x8246cc71ab01533b5bebc672a636df812f10637ad720797319d5741d5ebb3962'
        and contract_address in (
            select
                distinct(contract_address)
            from
                token_meta
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
AND tx_succeeded
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    l.liquidator,
    l.borrower,
    l.collateral_asset AS protocol_market,
    amc.underlying_address AS collateral_token,
    coalesce(w.amount_unadj, l.yield_balance) AS liquidated_amount_unadj,
    amd.underlying_address AS debt_token,
    l.repaid_amount_unadj AS repaid_amount_unadj,
    amc.protocol || '-' || amc.version AS platform,
    amc.protocol,
    amc.version,
    l._log_id,
    l.modified_timestamp,
    'Liquidate' AS event_name
FROM
    base l
    LEFT JOIN token_meta amc
    ON l.collateral_asset = amc.contract_address
    LEFT JOIN token_meta amd
    ON l.debt_asset = amd.contract_address 
    LEFT JOIN {{ ref('silver_lending__euler_withdraws') }} w
    ON l.tx_hash = w.tx_hash
    AND l.collateral_asset = w.contract_address
    AND l.liquidator = w.depositor 
WHERE amc.contract_address is not null qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
    l.modified_timestamp DESC)) = 1