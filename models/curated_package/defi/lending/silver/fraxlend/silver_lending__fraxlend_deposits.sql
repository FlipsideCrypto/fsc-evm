{# Get variables #}
{% set vars = return_vars() %}

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['silver','defi','lending','curated','fraxlend']
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
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS caller,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS owner,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS deposit_amount_unadj,
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
    topics [0] = '0xa32435755c235de2976ed44a75a2f85cb01faf0c894f639fe0c32bb9455fea8f'
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
  caller,
  owner as depositor,
  deposit_amount_unadj as amount_unadj,
  frax_market_address as protocol_market,
  frax_market_symbol as protocol_market_symbol,
  underlying_asset AS token_address,
  protocol,
  version,
  platform,
  _log_id,
  modified_timestamp,
  'AddCollateral' as event_name
FROM
  log_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    modified_timestamp DESC)) = 1
