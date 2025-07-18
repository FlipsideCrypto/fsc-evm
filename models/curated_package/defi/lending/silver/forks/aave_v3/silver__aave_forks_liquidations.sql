{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
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
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__aave_forks_tokens') }}
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
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
    collateral_asset,
    amc.atoken_address AS collateral_token,
    liquidated_amount AS amount_unadj,
    liquidated_amount / pow(
        10,
        amc.underlying_decimals
    ) AS amount,
    debt_asset,
    amd.atoken_address AS debt_token,
    liquidator_address AS liquidator,
    borrower_address AS borrower,
    amc.protocol || '-' || amc.version AS platform,
    amc.protocol,
    amc.version,
    amc.underlying_symbol AS collateral_token_symbol,
    amd.underlying_symbol AS debt_token_symbol,
    amc.underlying_decimals AS collateral_token_decimals,
    amd.underlying_decimals AS debt_token_decimals,
    'gnosis' AS blockchain,
    l._log_id,
    l._inserted_timestamp
FROM
    liquidation l
    INNER JOIN atoken_meta amc
    ON l.collateral_asset = amc.underlying_address
    INNER JOIN atoken_meta amd
    ON l.debt_asset = amd.underlying_address qualify(ROW_NUMBER() over(PARTITION BY l._log_id
ORDER BY
    l._inserted_timestamp DESC)) = 1
