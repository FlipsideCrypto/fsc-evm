{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated','compound','comp_v2']
) }}

WITH asset_details AS (

    SELECT
        token_address,
        token_symbol,
        token_name,
        token_decimals,
        underlying_asset_address,
        underlying_name,
        protocol,
        version
    FROM
        {{ ref('silver_lending__comp_v2_asset_details') }}
),
comp_v2_fork_liquidations AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
        contract_address AS protocol_market,
        asd2.underlying_asset_address AS collateral_token,
        asd1.underlying_asset_address AS debt_token,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS liquidator,
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INTEGER AS seizeTokens_raw,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS repayAmount_raw,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS tokenCollateral,
        asd1.protocol,
        asd1.version,
        asd1.protocol || '-' || asd1.version AS platform,
        modified_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        LEFT JOIN asset_details asd1
        ON contract_address = asd1.token_address
        LEFT JOIN asset_details asd2
        ON tokenCollateral = asd2.token_address
    WHERE
        topics [0] :: STRING = '0x298637f684da70674f26509b10f07ec2fbc77a335ab1e7d6215a4b2484d8bb52'
        AND tx_succeeded
        AND (contract_address in (select token_address from asset_details)
        AND tokenCollateral in (select token_address from asset_details))

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
exchange_rate AS (
    SELECT
        tx_hash,
        to_address,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output,
        TRY_CAST(utils.udf_hex_to_int(
            segmented_output[0] :: STRING
        ) AS FLOAT) AS output
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                comp_v2_fork_liquidations
        )
        AND input = '0x182df0f5'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tx_hash, to_address
        ORDER BY output DESC NULLS LAST
    ) = 1
)
SELECT
    block_number,
    block_timestamp,
    l.tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    liquidator,
    protocol_market,
    collateral_token,
    seizeTokens_raw * e.output / pow(
        10,
        18
    ) AS liquidated_amount_unadj,
    debt_token,
    repayAmount_raw AS repaid_amount_unadj,
    protocol,
    version,
    platform,
    _log_id,
    modified_timestamp AS _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    'LiquidateBorrow' AS event_name
FROM
    comp_v2_fork_liquidations l
    INNER JOIN exchange_rate e
    ON e.to_address = l.tokenCollateral
    AND e.tx_hash = l.tx_hash qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
