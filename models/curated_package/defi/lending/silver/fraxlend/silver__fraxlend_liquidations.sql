{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
      tags = ['silver','defi','lending','curated','fraxlend','fraxlend']
) }}

WITH log_join AS (

  SELECT
    l.tx_hash,
    l.block_timestamp,
    l.block_number,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    l.origin_from_address AS liquidator,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS borrower,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS collateral_for_liquidator_unadj,
    collateral_for_liquidator_unadj / pow(
      10,
      f.underlying_decimals
    ) AS collateral_for_liquidator,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER / pow(
      10,
      f.underlying_decimals
    ) AS shares_to_liquidate,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER  AS liquidator_repay_amount_unadj,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER / pow(
      10,
      f.underlying_decimals
    ) AS liquidator_repay_amount,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER / pow(
      10,
      f.underlying_decimals
    ) AS shares_to_adjust,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER / pow(
      10,
      f.underlying_decimals
    ) AS amount_to_adjust,
    liquidator_repay_amount / NULLIF(
      shares_to_liquidate,
      0
    ) AS liquidator_share_price,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
    f.underlying_symbol,
    f.underlying_decimals,
    f.protocol,
    f.version,
    f.platform,
    CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
    l.modified_timestamp
  FROM
    {{ ref('silver__fraxlend_asset_details') }}
    f
    LEFT JOIN {{ ref('core__fact_event_logs') }}
    l
    ON f.frax_market_address = l.contract_address
  WHERE
    topics [0] = '0x35f432a64bd3767447a456650432406c6cacb885819947a202216eeea6820ecf'
    AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
  SELECT
    MAX(
      modified_timestamp
    ) - INTERVAL '12 hours'
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  tx_hash,
  block_timestamp,
  block_number,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  liquidator,
  borrower,
  underlying_asset as collateral_token,
  underlying_symbol as collateral_token_symbol,
  collateral_for_liquidator_unadj as liquidated_amount_unadj,
  collateral_for_liquidator as liquidated_amount,
  shares_to_liquidate as shares_liquidated,
  LOWER('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS debt_token,
  'FRAX' AS debt_token_symbol,
  liquidator_repay_amount_unadj as repaid_amount_unadj,
  liquidator_repay_amount as repaid_amount,
  shares_to_adjust,
  amount_to_adjust,
  liquidator_share_price,
  underlying_decimals as collateral_token_decimals, 
  protocol,
  version,
  platform,
  _log_id,
  modified_timestamp,
  'Liquidate' AS event_name
FROM
  log_join l qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  modified_timestamp DESC)) = 1
