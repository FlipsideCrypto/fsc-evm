{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__poolcreated_evt_v3_pool_actions') }}

{% if is_incremental() and 'poolcreated_evt_v3' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__paircreated_evt_v2_pool_actions') }}

{% if is_incremental() and 'paircreated_evt_v2' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_dynamic_pool_actions') }}

{% if is_incremental() and 'kyberswap_v1_dynamic' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_pool_actions') }}

{% if is_incremental() and 'kyberswap_v2_elastic' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v1_pool_actions') }}

{% if is_incremental() and 'dodo_v1' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__pancakeswap_v3_pool_actions') }}

{% if is_incremental() and 'pancakeswap_v3' not in vars.CURATED_FR_MODELS %}
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
    type,
    _log_id AS _id,
    modified_timestamp AS _inserted_timestamp
  FROM
    {{ ref('silver_dex__balancer_pool_actions') }}

{% if is_incremental() and 'balancer' not in vars.CURATED_FR_MODELS %}
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
),
complete_lps AS (
  SELECT
    a.block_number,
    a.block_timestamp,
    a.tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    a.pool_address,
    lp.pool_name,
    a.token0,
    a.token1,
    a.token2,
    a.token3,
    a.token4,
    a.token5,
    a.token6,
    a.token7,
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
    a.amount0_unadj,
    a.amount1_unadj,
    a.amount2_unadj,
    a.amount3_unadj,
    a.amount4_unadj,
    a.amount5_unadj,
    a.amount6_unadj,
    a.amount7_unadj,
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
    a.platform,
    a.protocol,
    a.version,
    a.type,
    a._id,
    a._inserted_timestamp
  FROM
    all_pools a
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools')}} lp
    ON lp.pool_address = a.pool_address
    LEFT JOIN contracts c0
    ON c0.contract_address = a.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = a.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = a.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = a.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = a.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = a.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = a.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = a.token7
    LEFT JOIN prices p0
    ON a.token0 = p0.token_address AND DATE_TRUNC('hour',block_timestamp) = p0.hour
    LEFT JOIN prices p1
    ON a.token1 = p1.token_address AND DATE_TRUNC('hour',block_timestamp) = p1.hour
    LEFT JOIN prices p2
    ON a.token2 = p2.token_address AND DATE_TRUNC('hour',block_timestamp) = p2.hour
    LEFT JOIN prices p3
    ON a.token3 = p3.token_address AND DATE_TRUNC('hour',block_timestamp) = p3.hour
    LEFT JOIN prices p4
    ON a.token4 = p4.token_address AND DATE_TRUNC('hour',block_timestamp) = p4.hour
    LEFT JOIN prices p5
    ON a.token5 = p5.token_address AND DATE_TRUNC('hour',block_timestamp) = p5.hour
    LEFT JOIN prices p6
    ON a.token6 = p6.token_address AND DATE_TRUNC('hour',block_timestamp) = p6.hour
    LEFT JOIN prices p7
    ON a.token7 = p7.token_address AND DATE_TRUNC('hour',block_timestamp) = p7.hour
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
    t0.token0,
    t0.token1,
    t0.token2,
    t0.token3,
    t0.token4,
    t0.token5,
    t0.token6,
    t0.token7,
    c0.token_symbol AS token0_symbol_heal,
    c1.token_symbol AS token1_symbol_heal,
    c2.token_symbol AS token2_symbol_heal,
    c3.token_symbol AS token3_symbol_heal,
    c4.token_symbol AS token4_symbol_heal,
    c5.token_symbol AS token5_symbol_heal,
    c6.token_symbol AS token6_symbol_heal,
    c7.token_symbol AS token7_symbol_heal,
    c0.token_decimals AS token0_decimals_heal,
    c1.token_decimals AS token1_decimals_heal,
    c2.token_decimals AS token2_decimals_heal,
    c3.token_decimals AS token3_decimals_heal,
    c4.token_decimals AS token4_decimals_heal,
    c5.token_decimals AS token5_decimals_heal,
    c6.token_decimals AS token6_decimals_heal,
    c7.token_decimals AS token7_decimals_heal,
    t0.amount0_unadj,
    t0.amount1_unadj,
    t0.amount2_unadj,
    t0.amount3_unadj,
    t0.amount4_unadj,
    t0.amount5_unadj,
    t0.amount6_unadj,
    t0.amount7_unadj,
    CASE
      WHEN token0_decimals_heal IS NULL THEN amount0_unadj
      ELSE (amount0_unadj / pow(10, token0_decimals_heal))
    END AS amount0_heal,
    CASE
      WHEN token1_decimals_heal IS NULL THEN amount1_unadj
      ELSE (amount1_unadj / pow(10, token1_decimals_heal))
    END AS amount1_heal,
    CASE
      WHEN token2_decimals_heal IS NULL THEN amount2_unadj
      ELSE (amount2_unadj / pow(10, token2_decimals_heal))
    END AS amount2_heal,
    CASE
      WHEN token3_decimals_heal IS NULL THEN amount3_unadj
      ELSE (amount3_unadj / pow(10, token3_decimals_heal))
    END AS amount3_heal,
    CASE
      WHEN token4_decimals_heal IS NULL THEN amount4_unadj
      ELSE (amount4_unadj / pow(10, token4_decimals_heal))
    END AS amount4_heal,
    CASE
      WHEN token5_decimals_heal IS NULL THEN amount5_unadj
      ELSE (amount5_unadj / pow(10, token5_decimals_heal))
    END AS amount5_heal,
    CASE
      WHEN token6_decimals_heal IS NULL THEN amount6_unadj
      ELSE (amount6_unadj / pow(10, token6_decimals_heal))
    END AS amount6_heal,
    CASE
      WHEN token7_decimals_heal IS NULL THEN amount7_unadj
      ELSE (amount7_unadj / pow(10, token7_decimals_heal))
    END AS amount7_heal,
    CASE
      WHEN token0_decimals_heal IS NOT NULL THEN amount0_heal * p0.price
      ELSE NULL
    END AS amount0_usd_heal,
    CASE
      WHEN token1_decimals_heal IS NOT NULL THEN amount1_heal * p1.price
      ELSE NULL
    END AS amount1_usd_heal,
    CASE
      WHEN token2_decimals_heal IS NOT NULL THEN amount2_heal * p2.price
      ELSE NULL
    END AS amount2_usd_heal,
    CASE
      WHEN token3_decimals_heal IS NOT NULL THEN amount3_heal * p3.price
      ELSE NULL
    END AS amount3_usd_heal,
    CASE
      WHEN token4_decimals_heal IS NOT NULL THEN amount4_heal * p4.price
      ELSE NULL
    END AS amount4_usd_heal,
    CASE
      WHEN token5_decimals_heal IS NOT NULL THEN amount5_heal * p5.price
      ELSE NULL
    END AS amount5_usd_heal,
    CASE
      WHEN token6_decimals_heal IS NOT NULL THEN amount6_heal * p6.price
      ELSE NULL
    END AS amount6_usd_heal,
    CASE
      WHEN token7_decimals_heal IS NOT NULL THEN amount7_heal * p7.price
      ELSE NULL
    END AS amount7_usd_heal,
    p0.is_verified AS token0_is_verified_heal,
    p1.is_verified AS token1_is_verified_heal,
    p2.is_verified AS token2_is_verified_heal,
    p3.is_verified AS token3_is_verified_heal,
    p4.is_verified AS token4_is_verified_heal,
    p5.is_verified AS token5_is_verified_heal,
    p6.is_verified AS token6_is_verified_heal,
    p7.is_verified AS token7_is_verified_heal,
    t0.platform,
    t0.protocol,
    t0.version,
    t0.type,
    t0._id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools')}} lp
    ON lp.pool_address = t0.pool_address
    LEFT JOIN contracts c0
    ON c0.contract_address = t0.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = t0.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = t0.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = t0.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = t0.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = t0.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = t0.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = t0.token7
    LEFT JOIN prices p0
    ON t0.token0 = p0.token_address AND DATE_TRUNC('hour',block_timestamp) = p0.hour
    LEFT JOIN prices p1
    ON t0.token1 = p1.token_address AND DATE_TRUNC('hour',block_timestamp) = p1.hour
    LEFT JOIN prices p2
    ON t0.token2 = p2.token_address AND DATE_TRUNC('hour',block_timestamp) = p2.hour
    LEFT JOIN prices p3
    ON t0.token3 = p3.token_address AND DATE_TRUNC('hour',block_timestamp) = p3.hour
    LEFT JOIN prices p4
    ON t0.token4 = p4.token_address AND DATE_TRUNC('hour',block_timestamp) = p4.hour
    LEFT JOIN prices p5
    ON t0.token5 = p5.token_address AND DATE_TRUNC('hour',block_timestamp) = p5.hour
    LEFT JOIN prices p6
    ON t0.token6 = p6.token_address AND DATE_TRUNC('hour',block_timestamp) = p6.hour
    LEFT JOIN prices p7
    ON t0.token7 = p7.token_address AND DATE_TRUNC('hour',block_timestamp) = p7.hour
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
        AND t1.token0 IN (
          SELECT token_address
          FROM {{ ref('price__ez_asset_metadata') }}
          WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
            AND t2.token1 IN (
              SELECT token_address
              FROM {{ ref('price__ez_asset_metadata') }}
              WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                AND t3.token2 IN (
                  SELECT token_address
                  FROM {{ ref('price__ez_asset_metadata') }}
                  WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                    AND t4.token3 IN (
                      SELECT token_address
                      FROM {{ ref('price__ez_asset_metadata') }}
                      WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                          AND t5.token4 IN (
                            SELECT token_address
                            FROM {{ ref('price__ez_asset_metadata') }}
                            WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                            AND t6.token5 IN (
                              SELECT token_address
                              FROM {{ ref('price__ez_asset_metadata') }}
                              WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                                  AND t7.token6 IN (
                                    SELECT token_address
                                    FROM {{ ref('price__ez_asset_metadata') }}
                                    WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
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
                                      AND t8.token7 IN (
                                        SELECT token_address
                                        FROM {{ ref('price__ez_asset_metadata') }}
                                        WHERE IFNULL(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -8, (SELECT MAX(_inserted_timestamp) :: DATE FROM {{ this }})) -- newly verified token
                                      )
                                      )
                                    CONCAT(
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
                                          {{ ref('silver_dex__complete_dex_liquidity_pools')}} lp
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
                                  *
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
  token0,
  token1,
  token2,
  token3,
  token4,
  token5,
  token6,
  token7,
  token0_symbol_heal AS token0_symbol,
  token1_symbol_heal AS token1_symbol,
  token2_symbol_heal AS token2_symbol,
  token3_symbol_heal AS token3_symbol,
  token4_symbol_heal AS token4_symbol,
  token5_symbol_heal AS token5_symbol,
  token6_symbol_heal AS token6_symbol,
  token7_symbol_heal AS token7_symbol,
  token0_decimals_heal AS token0_decimals,
  token1_decimals_heal AS token1_decimals,
  token2_decimals_heal AS token2_decimals,
  token3_decimals_heal AS token3_decimals,
  token4_decimals_heal AS token4_decimals,
  token5_decimals_heal AS token5_decimals,
  token6_decimals_heal AS token6_decimals,
  token7_decimals_heal AS token7_decimals,
  amount0_unadj,
  amount1_unadj,
  amount2_unadj,
  amount3_unadj,
  amount4_unadj,
  amount5_unadj,
  amount6_unadj,
  amount7_unadj,
  amount0_heal AS amount0,
  amount1_heal AS amount1,
  amount2_heal AS amount2,
  amount3_heal AS amount3,
  amount4_heal AS amount4,
  amount5_heal AS amount5,
  amount6_heal AS amount6,
  amount7_heal AS amount7,
  amount0_usd_heal AS amount0_usd,
  amount1_usd_heal AS amount1_usd,
  amount2_usd_heal AS amount2_usd,
  amount3_usd_heal AS amount3_usd,
  amount4_usd_heal AS amount4_usd,
  amount5_usd_heal AS amount5_usd,
  amount6_usd_heal AS amount6_usd,
  amount7_usd_heal AS amount7_usd,
  token0_is_verified_heal AS token0_is_verified,
  token1_is_verified_heal AS token1_is_verified,
  token2_is_verified_heal AS token2_is_verified,
  token3_is_verified_heal AS token3_is_verified,
  token4_is_verified_heal AS token4_is_verified,
  token5_is_verified_heal AS token5_is_verified,
  token6_is_verified_heal AS token6_is_verified,
  token7_is_verified_heal AS token7_is_verified,
  platform,
  protocol,
  version,
  type,
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
  token0,
  token1,
  token2,
  token3,
  token4,
  token5,
  token6,
  token7,
  token0_symbol,
  token1_symbol,
  token2_symbol,
  token3_symbol,
  token4_symbol,
  token5_symbol,
  token6_symbol,
  token7_symbol,
  token0_decimals,
  token1_decimals,
  token2_decimals,
  token3_decimals,
  token4_decimals,
  token5_decimals,
  token6_decimals,
  token7_decimals,
  amount0_unadj,
  amount1_unadj,
  amount2_unadj,
  amount3_unadj,
  amount4_unadj,
  amount5_unadj,
  amount6_unadj,
  amount7_unadj,
  amount0,
  amount1,
  amount2,
  amount3,
  amount4,
  amount5,
  amount6,
  amount7,
  amount0_usd,
  amount1_usd,
  amount2_usd,
  amount3_usd,
  amount4_usd,
  amount5_usd,
  amount6_usd,
  amount7_usd,
  token0_is_verified,
  token1_is_verified,
  token2_is_verified,
  token3_is_verified,
  token4_is_verified,
  token5_is_verified,
  token6_is_verified,
  token7_is_verified,
  platform,
  protocol,
  version,
  type,
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
