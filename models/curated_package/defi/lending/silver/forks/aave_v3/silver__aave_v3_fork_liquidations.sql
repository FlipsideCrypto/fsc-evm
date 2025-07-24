{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','liquidations']
) }}

WITH atoken_meta AS (
    SELECT
        atoken_created_block,
        version_pool,
        treasury_address,
        atoken_symbol,
        atoken_address,
        token_stable_debt_address,
        token_variable_debt_address,
        atoken_decimals,
        atoken_version,
        atoken_name,
        underlying_symbol,
        underlying_address,
        underlying_decimals,
        underlying_name,
        protocol,
        version,
        modified_timestamp,
        _log_id
    FROM
        {{ ref('silver__aave_v3_fork_tokens') }}
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
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286'

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
        atoken_meta
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
    collateral_asset AS collateral_token,
    amc.underlying_symbol AS collateral_token_symbol,
    liquidated_amount AS liquidated_amount_unadj,
    liquidated_amount / pow(
        10,
        amc.underlying_decimals
    ) AS liquidated_amount,
    debt_asset AS debt_token,
    amd.underlying_symbol AS debt_token_symbol,
    debt_to_cover_amount AS repaid_amount_unadj,
    debt_to_cover_amount / pow(
        10,
        amd.underlying_decimals
    ) AS repaid_amount,
    amc.protocol || '-' || amc.version AS platform,
    amc.protocol,
    amc.version,
    l._log_id,
    l.modified_timestamp
FROM
    liquidation l
    INNER JOIN atoken_meta amc
    ON l.collateral_asset = amc.underlying_address
    INNER JOIN atoken_meta amd
    ON l.debt_asset = amd.underlying_address qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
    l.modified_timestamp DESC)) = 1
