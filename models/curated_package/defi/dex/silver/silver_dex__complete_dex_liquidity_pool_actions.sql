{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, pool_address, pool_name)",
  tags = ['silver_dex','defi','dex','curated','heal','complete','lp_actions']
) }}

WITH contracts AS (

  SELECT
    address AS contract_address,
    symbol AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS contract_address,
    '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  WHERE
    address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
prices AS (
  SELECT
    token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  WHERE
    token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
poolcreated_evt_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__poolcreated_evt_v3_pool_actions') }}

{% if is_incremental() and 'poolcreated_evt_v3' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
paircreated_evt_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__paircreated_evt_v2_pool_actions') }}

{% if is_incremental() and 'paircreated_evt_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
aerodrome AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__aerodrome_pool_actions') }}

{% if is_incremental() and 'aerodrome' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pharaoh_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pharaoh_v1_pool_actions') }}

{% if is_incremental() and 'pharaoh_v1' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v1_dynamic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_pool_actions') }}

{% if is_incremental() and 'kyberswap_v1_dynamic' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v2_elastic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_pool_actions') }}

{% if is_incremental() and 'kyberswap_v2_elastic' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dodo_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v1_pool_actions') }}

{% if is_incremental() and 'dodo_v1' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
pancakeswap_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v3_pool_actions') }}

{% if is_incremental() and 'pancakeswap_v3' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
camelot_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__camelot_v2_pool_actions') }}

{% if is_incremental() and 'camelot_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
quickswap_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__quickswap_v2_pool_actions') }}

{% if is_incremental() and 'quickswap_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dackie AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dackie_pool_actions') }}

{% if is_incremental() and 'dackie' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
superchain_slipstream AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__superchain_slipstream_pool_actions') }}

{% if is_incremental() and 'superchain_slipstream' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
sushiswap AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushiswap_pool_actions') }}

{% if is_incremental() and 'sushiswap' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
trader_joe_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__trader_joe_v2_pool_actions') }}

{% if is_incremental() and 'trader_joe_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
zyberswap_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__zyberswap_v2_pool_actions') }}

{% if is_incremental() and 'zyberswap_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome_v1 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__velodrome_v1_pool_actions') }}

{% if is_incremental() and 'velodrome_v1' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    NULL AS token2,
    NULL AS token3,
    NULL AS token4,
    NULL AS token5,
    NULL AS token6,
    NULL AS token7,
    amount0_unadj,
    amount1_unadj,
    NULL AS amount2_unadj,
    NULL AS amount3_unadj,
    NULL AS amount4_unadj,
    NULL AS amount5_unadj,
    NULL AS amount6_unadj,
    NULL AS amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__velodrome_v2_pool_actions') }}

{% if is_incremental() and 'velodrome_v2' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
balancer AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    amount0_unadj,
    amount1_unadj,
    amount2_unadj,
    amount3_unadj,
    amount4_unadj,
    amount5_unadj,
    amount6_unadj,
    amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__balancer_pool_actions') }}

{% if is_incremental() and 'balancer' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
curve AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    token0,
    token1,
    token2,
    token3,
    token4,
    token5,
    token6,
    token7,
    amount0_unadj,
    amount1_unadj,
    amount2_unadj,
    amount3_unadj,
    amount4_unadj,
    amount5_unadj,
    amount6_unadj,
    amount7_unadj,
    platform,
    protocol,
    version,
    TYPE,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_pool_actions') }}

{% if is_incremental() and 'curve' not in vars.curated_fr_models %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
all_pools AS (
  SELECT
    *
  FROM
    poolcreated_evt_v3
  UNION ALL
  SELECT
    *
  FROM
    paircreated_evt_v2
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v1_dynamic
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v2_elastic
  UNION ALL
  SELECT
    *
  FROM
    dodo_v1
  UNION ALL
  SELECT
    *
  FROM
    pancakeswap_v3
  UNION ALL
  SELECT
    *
  FROM
    balancer
  UNION ALL
  SELECT
    *
  FROM
    curve
  UNION ALL
  SELECT
    *
  FROM
    aerodrome
  UNION ALL
  SELECT
    *
  FROM
    camelot_v2
  UNION ALL
  SELECT
    *
  FROM
    dackie
  UNION ALL
  SELECT
    *
  FROM
    pharaoh_v1
  UNION ALL
  SELECT
    *
  FROM
    quickswap_v2
  UNION ALL
  SELECT
    *
  FROM
    superchain_slipstream
  UNION ALL
  SELECT
    *
  FROM
    sushiswap
  UNION ALL
  SELECT
    *
  FROM
    trader_joe_v2
  UNION ALL
  SELECT
    *
  FROM
    zyberswap_v2
  UNION ALL
  SELECT
    *
  FROM
    velodrome_v1
  UNION ALL
  SELECT
    *
  FROM
    velodrome_v2
),
complete_lps AS (
  SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    A.pool_address,
    lp.pool_name,
    A.token0,
    A.token1,
    A.token2,
    A.token3,
    A.token4,
    A.token5,
    A.token6,
    A.token7,
    c0.token_symbol AS token0_symbol,
    c1.token_symbol AS token1_symbol,
    c2.token_symbol AS token2_symbol,
    c3.token_symbol AS token3_symbol,
    c4.token_symbol AS token4_symbol,
    c5.token_symbol AS token5_symbol,
    c6.token_symbol AS token6_symbol,
    c7.token_symbol AS token7_symbol,
    c0.token_decimals AS token0_decimals,
    c1.token_decimals AS token1_decimals,
    c2.token_decimals AS token2_decimals,
    c3.token_decimals AS token3_decimals,
    c4.token_decimals AS token4_decimals,
    c5.token_decimals AS token5_decimals,
    c6.token_decimals AS token6_decimals,
    c7.token_decimals AS token7_decimals,
    A.amount0_unadj,
    A.amount1_unadj,
    A.amount2_unadj,
    A.amount3_unadj,
    A.amount4_unadj,
    A.amount5_unadj,
    A.amount6_unadj,
    A.amount7_unadj,
    CASE
      WHEN token0_decimals IS NULL THEN amount0_unadj
      ELSE (amount0_unadj / pow(10, token0_decimals))
    END AS amount0,
    CASE
      WHEN token1_decimals IS NULL THEN amount1_unadj
      ELSE (amount1_unadj / pow(10, token1_decimals))
    END AS amount1,
    CASE
      WHEN token2_decimals IS NULL THEN amount2_unadj
      ELSE (amount2_unadj / pow(10, token2_decimals))
    END AS amount2,
    CASE
      WHEN token3_decimals IS NULL THEN amount3_unadj
      ELSE (amount3_unadj / pow(10, token3_decimals))
    END AS amount3,
    CASE
      WHEN token4_decimals IS NULL THEN amount4_unadj
      ELSE (amount4_unadj / pow(10, token4_decimals))
    END AS amount4,
    CASE
      WHEN token5_decimals IS NULL THEN amount5_unadj
      ELSE (amount5_unadj / pow(10, token5_decimals))
    END AS amount5,
    CASE
      WHEN token6_decimals IS NULL THEN amount6_unadj
      ELSE (amount6_unadj / pow(10, token6_decimals))
    END AS amount6,
    CASE
      WHEN token7_decimals IS NULL THEN amount7_unadj
      ELSE (amount7_unadj / pow(10, token7_decimals))
    END AS amount7,
    CASE
      WHEN token0_decimals IS NOT NULL THEN amount0 * p0.price
      ELSE NULL
    END AS amount0_usd,
    CASE
      WHEN token1_decimals IS NOT NULL THEN amount1 * p1.price
      ELSE NULL
    END AS amount1_usd,
    CASE
      WHEN token2_decimals IS NOT NULL THEN amount2 * p2.price
      ELSE NULL
    END AS amount2_usd,
    CASE
      WHEN token3_decimals IS NOT NULL THEN amount3 * p3.price
      ELSE NULL
    END AS amount3_usd,
    CASE
      WHEN token4_decimals IS NOT NULL THEN amount4 * p4.price
      ELSE NULL
    END AS amount4_usd,
    CASE
      WHEN token5_decimals IS NOT NULL THEN amount5 * p5.price
      ELSE NULL
    END AS amount5_usd,
    CASE
      WHEN token6_decimals IS NOT NULL THEN amount6 * p6.price
      ELSE NULL
    END AS amount6_usd,
    CASE
      WHEN token7_decimals IS NOT NULL THEN amount7 * p7.price
      ELSE NULL
    END AS amount7_usd,
    p0.is_verified AS token0_is_verified,
    p1.is_verified AS token1_is_verified,
    p2.is_verified AS token2_is_verified,
    p3.is_verified AS token3_is_verified,
    p4.is_verified AS token4_is_verified,
    p5.is_verified AS token5_is_verified,
    p6.is_verified AS token6_is_verified,
    p7.is_verified AS token7_is_verified,
    A.platform,
    A.protocol,
    A.version,
    A.type,
    A._id,
    A._inserted_timestamp
  FROM
    all_pools A
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON lp.pool_address = A.pool_address
    LEFT JOIN contracts c0
    ON c0.contract_address = A.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = A.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = A.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = A.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = A.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = A.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = A.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = A.token7
    LEFT JOIN prices p0
    ON A.token0 = p0.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p0.hour
    LEFT JOIN prices p1
    ON A.token1 = p1.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON A.token2 = p2.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p2.hour
    LEFT JOIN prices p3
    ON A.token3 = p3.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p3.hour
    LEFT JOIN prices p4
    ON A.token4 = p4.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p4.hour
    LEFT JOIN prices p5
    ON A.token5 = p5.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p5.hour
    LEFT JOIN prices p6
    ON A.token6 = p6.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p6.hour
    LEFT JOIN prices p7
    ON A.token7 = p7.token_address
    AND DATE_TRUNC(
      'hour',
      A.block_timestamp
    ) = p7.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    t0.pool_address,
    lp.pool_name AS pool_name_heal,
    t0.tokens,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'token0',
      c0.token_symbol,
      'token1',
      c1.token_symbol,
      'token2',
      c2.token_symbol,
      'token3',
      c3.token_symbol,
      'token4',
      c4.token_symbol,
      'token5',
      c5.token_symbol,
      'token6',
      c6.token_symbol,
      'token7',
      c7.token_symbol
    ) AS symbols_heal,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'token0',
      c0.token_decimals,
      'token1',
      c1.token_decimals,
      'token2',
      c2.token_decimals,
      'token3',
      c3.token_decimals,
      'token4',
      c4.token_decimals,
      'token5',
      c5.token_decimals,
      'token6',
      c6.token_decimals,
      'token7',
      c7.token_decimals
    ) AS decimals_heal,
    t0.amounts_unadj,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'token0',
      CASE
        WHEN c0.token_decimals IS NULL THEN t0.amounts_unadj :token0 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token0) / pow(10, c0.token_decimals)) :: STRING
      END,
      'token1',
      CASE
        WHEN c1.token_decimals IS NULL THEN t0.amounts_unadj :token1 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token1) / pow(10, c1.token_decimals)) :: STRING
      END,
      'token2',
      CASE
        WHEN c2.token_decimals IS NULL THEN t0.amounts_unadj :token2 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token2) / pow(10, c2.token_decimals)) :: STRING
      END,
      'token3',
      CASE
        WHEN c3.token_decimals IS NULL THEN t0.amounts_unadj :token3 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token3) / pow(10, c3.token_decimals)) :: STRING
      END,
      'token4',
      CASE
        WHEN c4.token_decimals IS NULL THEN t0.amounts_unadj :token4 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token4) / pow(10, c4.token_decimals)) :: STRING
      END,
      'token5',
      CASE
        WHEN c5.token_decimals IS NULL THEN t0.amounts_unadj :token5 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token5) / pow(10, c5.token_decimals)) :: STRING
      END,
      'token6',
      CASE
        WHEN c6.token_decimals IS NULL THEN t0.amounts_unadj :token6 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token6) / pow(10, c6.token_decimals)) :: STRING
      END,
      'token7',
      CASE
        WHEN c7.token_decimals IS NULL THEN t0.amounts_unadj :token7 :: STRING
        ELSE (TRY_TO_NUMBER(t0.amounts_unadj :token7) / pow(10, c7.token_decimals)) :: STRING
      END
    ) AS amounts_heal,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'token0',
      CASE
        WHEN c0.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token0) * p0.price) :: STRING
        ELSE NULL
      END,
      'token1',
      CASE
        WHEN c1.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token1) * p1.price) :: STRING
        ELSE NULL
      END,
      'token2',
      CASE
        WHEN c2.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token2) * p2.price) :: STRING
        ELSE NULL
      END,
      'token3',
      CASE
        WHEN c3.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token3) * p3.price) :: STRING
        ELSE NULL
      END,
      'token4',
      CASE
        WHEN c4.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token4) * p4.price) :: STRING
        ELSE NULL
      END,
      'token5',
      CASE
        WHEN c5.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token5) * p5.price) :: STRING
        ELSE NULL
      END,
      'token6',
      CASE
        WHEN c6.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token6) * p6.price) :: STRING
        ELSE NULL
      END,
      'token7',
      CASE
        WHEN c7.token_decimals IS NOT NULL THEN (TRY_TO_NUMBER(t0.amounts_heal :token7) * p7.price) :: STRING
        ELSE NULL
      END
    ) AS amounts_usd_heal,
    OBJECT_CONSTRUCT_KEEP_NULL(
      'token0',
      p0.is_verified,
      'token1',
      p1.is_verified,
      'token2',
      p2.is_verified,
      'token3',
      p3.is_verified,
      'token4',
      p4.is_verified,
      'token5',
      p5.is_verified,
      'token6',
      p6.is_verified,
      'token7',
      p7.is_verified
    ) AS tokens_is_verified_heal,
    t0.platform,
    t0.protocol,
    t0.version,
    t0.type,
    t0._id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON lp.pool_address = t0.pool_address
    LEFT JOIN contracts c0
    ON c0.contract_address = t0.tokens :token0
    LEFT JOIN contracts c1
    ON c1.contract_address = t0.tokens :token1
    LEFT JOIN contracts c2
    ON c2.contract_address = t0.tokens :token2
    LEFT JOIN contracts c3
    ON c3.contract_address = t0.tokens :token3
    LEFT JOIN contracts c4
    ON c4.contract_address = t0.tokens :token4
    LEFT JOIN contracts c5
    ON c5.contract_address = t0.tokens :token5
    LEFT JOIN contracts c6
    ON c6.contract_address = t0.tokens :token6
    LEFT JOIN contracts c7
    ON c7.contract_address = t0.tokens :token7
    LEFT JOIN prices p0
    ON t0.tokens :token0 = p0.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p0.hour
    LEFT JOIN prices p1
    ON t0.tokens :token1 = p1.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON t0.tokens :token2 = p2.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p2.hour
    LEFT JOIN prices p3
    ON t0.tokens :token3 = p3.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p3.hour
    LEFT JOIN prices p4
    ON t0.tokens :token4 = p4.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p4.hour
    LEFT JOIN prices p5
    ON t0.tokens :token5 = p5.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p5.hour
    LEFT JOIN prices p6
    ON t0.tokens :token6 = p6.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p6.hour
    LEFT JOIN prices p7
    ON t0.tokens :token7 = p7.token_address
    AND DATE_TRUNC(
      'hour',
      t0.block_timestamp
    ) = p7.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t1.tokens :token0 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t2.block_number,
          '-',
          t2.platform,
          '-',
          t2.version
        )
      FROM
        {{ this }}
        t2
      WHERE
        t2._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t2.tokens :token1 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t3.block_number,
          '-',
          t3.platform,
          '-',
          t3.version
        )
      FROM
        {{ this }}
        t3
      WHERE
        t3._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t3.tokens :token2 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t4.block_number,
          '-',
          t4.platform,
          '-',
          t4.version
        )
      FROM
        {{ this }}
        t4
      WHERE
        t4._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t4.tokens :token3 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t5.block_number,
          '-',
          t5.platform,
          '-',
          t5.version
        )
      FROM
        {{ this }}
        t5
      WHERE
        t5._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t5.tokens :token4 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t6.block_number,
          '-',
          t6.platform,
          '-',
          t6.version
        )
      FROM
        {{ this }}
        t6
      WHERE
        t6._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t6.tokens :token5 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t7.block_number,
          '-',
          t7.platform,
          '-',
          t7.version
        )
      FROM
        {{ this }}
        t7
      WHERE
        t7._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t7.tokens :token6 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t8.block_number,
          '-',
          t8.platform,
          '-',
          t8.version
        )
      FROM
        {{ this }}
        t8
      WHERE
        t8._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND t8.tokens :token7 IN (
          SELECT
            token_address
          FROM
            {{ ref('price__ez_asset_metadata') }}
          WHERE
            IFNULL(
              is_verified_modified_timestamp,
              '1970-01-01' :: TIMESTAMP
            ) > DATEADD(
              'day',
              -8,
              (
                SELECT
                  MAX(_inserted_timestamp) :: DATE
                FROM
                  {{ this }}
              )
            ) -- newly verified token
        )
    )
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t9.block_number,
          '-',
          t9.platform,
          '-',
          t9.version
        )
      FROM
        {{ this }}
        t9
      WHERE
        t9.pool_name IS NULL
        AND t9._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver_dex__complete_dex_liquidity_pools') }}
            lp
          WHERE
            lp._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND lp.pool_name IS NOT NULL
            AND lp.pool_address = t9.pool_address)
          GROUP BY
            1
        )
    ),
  {% endif %}

  FINAL AS (
    SELECT
      block_number,
      block_timestamp,
      tx_hash,
      event_index,
      event_name,
      liquidity_provider,
      sender,
      receiver,
      pool_address,
      pool_name,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        token0,
        'token1',
        token1,
        'token2',
        token2,
        'token3',
        token3,
        'token4',
        token4,
        'token5',
        token5,
        'token6',
        token6,
        'token7',
        token7
      ) AS tokens,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        token0_symbol,
        'token1',
        token1_symbol,
        'token2',
        token2_symbol,
        'token3',
        token3_symbol,
        'token4',
        token4_symbol,
        'token5',
        token5_symbol,
        'token6',
        token6_symbol,
        'token7',
        token7_symbol
      ) AS symbols,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        token0_decimals,
        'token1',
        token1_decimals,
        'token2',
        token2_decimals,
        'token3',
        token3_decimals,
        'token4',
        token4_decimals,
        'token5',
        token5_decimals,
        'token6',
        token6_decimals,
        'token7',
        token7_decimals
      ) AS decimals,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        amount0_unadj :: STRING,
        'token1',
        amount1_unadj :: STRING,
        'token2',
        amount2_unadj :: STRING,
        'token3',
        amount3_unadj :: STRING,
        'token4',
        amount4_unadj :: STRING,
        'token5',
        amount5_unadj :: STRING,
        'token6',
        amount6_unadj :: STRING,
        'token7',
        amount7_unadj :: STRING
      ) AS amounts_unadj,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        amount0 :: STRING,
        'token1',
        amount1 :: STRING,
        'token2',
        amount2 :: STRING,
        'token3',
        amount3 :: STRING,
        'token4',
        amount4 :: STRING,
        'token5',
        amount5 :: STRING,
        'token6',
        amount6 :: STRING,
        'token7',
        amount7 :: STRING
      ) AS amounts,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        amount0_usd :: STRING,
        'token1',
        amount1_usd :: STRING,
        'token2',
        amount2_usd :: STRING,
        'token3',
        amount3_usd :: STRING,
        'token4',
        amount4_usd :: STRING,
        'token5',
        amount5_usd :: STRING,
        'token6',
        amount6_usd :: STRING,
        'token7',
        amount7_usd :: STRING
      ) AS amounts_usd,
      OBJECT_CONSTRUCT_KEEP_NULL(
        'token0',
        token0_is_verified,
        'token1',
        token1_is_verified,
        'token2',
        token2_is_verified,
        'token3',
        token3_is_verified,
        'token4',
        token4_is_verified,
        'token5',
        token5_is_verified,
        'token6',
        token6_is_verified,
        'token7',
        token7_is_verified
      ) AS tokens_is_verified,
      platform,
      protocol,
      version,
      TYPE,
      _id,
      _inserted_timestamp
    FROM
      complete_lps

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  event_name,
  liquidity_provider,
  sender,
  receiver,
  pool_address,
  pool_name_heal AS pool_name,
  tokens,
  symbols_heal AS symbols,
  decimals_heal AS decimals,
  amounts_unadj_heal AS amounts_unadj,
  amounts_heal AS amounts,
  amounts_usd_heal AS amounts_usd,
  tokens_is_verified_heal AS tokens_is_verified,
  platform,
  protocol,
  version,
  TYPE,
  _id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  event_name,
  liquidity_provider,
  sender,
  receiver,
  pool_address,
  pool_name,
  tokens,
  symbols,
  decimals,
  amounts_unadj,
  amounts,
  amounts_usd,
  tokens_is_verified,
  platform,
  protocol,
  version,
  TYPE,
  _id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['_id']
  ) }} AS complete_dex_liquidity_pool_actions_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
