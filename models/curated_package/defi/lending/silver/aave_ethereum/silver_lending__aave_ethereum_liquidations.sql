{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','aave_ethereum']
) }}

WITH 
token_meta AS (

    SELECT
        atoken_created_block,
        version_pool,
        atoken_address,
        underlying_address,
        protocol,
        version,
        modified_timestamp,
        _log_id
    FROM
        {{ ref('silver_lending__aave_ethereum_tokens') }}
),
liquidation AS(
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateral_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS debt_asset,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS debt_to_cover_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS liquidated_amount,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS liquidator_address,
        COALESCE(
            contract_address,
            origin_to_address
        ) AS lending_pool_contract,
        CASE
            WHEN debt_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE debt_asset
        END AS debt_token,
        CASE
            WHEN collateral_asset = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE collateral_asset
        END AS collateral_token,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286',
            '0x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'
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
AND contract_address IN (
    SELECT
        DISTINCT(version_pool)
    FROM
        token_meta
)
AND tx_succeeded
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator_address AS liquidator,
    borrower_address AS borrower,
    amc.atoken_address AS protocol_market,
    l.collateral_token,
    liquidated_amount AS liquidated_amount_unadj,
    l.debt_token,
    debt_to_cover_amount AS repaid_amount_unadj,
    amc.protocol || '-' || amc.version AS platform,
    amc.protocol,
    amc.version,
    l._log_id,
    l.modified_timestamp,
    'LiquidationCall' AS event_name
FROM
    liquidation l
    LEFT JOIN token_meta amc
    on l.collateral_token = amc.underlying_address
    and l.lending_pool_contract = amc.version_pool qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
    l.modified_timestamp DESC)) = 1
