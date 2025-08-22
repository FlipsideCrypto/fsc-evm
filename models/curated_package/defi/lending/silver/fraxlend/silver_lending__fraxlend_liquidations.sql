{# Get variables #}
{% set vars = return_vars() %}

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
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS shares_to_liquidate_raw,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER  AS liquidator_repay_amount_unadj,
    utils.udf_hex_to_int(
      segmented_data [3] :: STRING
    ) :: INTEGER AS shares_to_adjust_raw,
    utils.udf_hex_to_int(
      segmented_data [4] :: STRING
    ) :: INTEGER AS amount_to_adjust_raw,
    f.frax_market_address,
    f.frax_market_symbol,
    f.underlying_asset,
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
            {{ ref('silver_lending__fraxlend_asset_details') }}
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
    MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
  FROM
    {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
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
  contract_address as protocol_market,
  liquidator,
  borrower,
  underlying_asset as collateral_token,
  collateral_for_liquidator_unadj as liquidated_amount_unadj,
  shares_to_liquidate_raw as shares_liquidated_unadj,
  LOWER('0x853d955aCEf822Db058eb8505911ED77F175b99e') AS debt_token,
  'FRAX' AS debt_token_symbol,
  liquidator_repay_amount_unadj as repaid_amount_unadj,
  shares_to_adjust_raw as shares_to_adjust_unadj,
  amount_to_adjust_raw as amount_to_adjust_unadj,
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
