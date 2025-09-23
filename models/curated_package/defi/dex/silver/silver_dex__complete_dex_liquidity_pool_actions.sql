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
    p.block_number,
    p.block_timestamp,
    p.tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    p.pool_address,
    lp.pool_name,
    p.token0,
    p.token1,
    p.token2,
    p.token3,
    p.token4,
    p.token5,
    p.token6,
    p.token7,
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
    p.amount0_unadj,
    p.amount1_unadj,
    p.amount2_unadj,
    p.amount3_unadj,
    p.amount4_unadj,
    p.amount5_unadj,
    p.amount6_unadj,
    p.amount7_unadj,
    --potentially group all as array?
    p.platform,
    p.protocol,
    p.version,
    p.type,
    p._id,
    p._inserted_timestamp
  FROM
    all_pools p
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools')}} lp
    ON lp.pool_address = p.pool_address
    LEFT JOIN contracts c0
    ON c0.contract_address = p.token0
    LEFT JOIN contracts c1
    ON c1.contract_address = p.token1
    LEFT JOIN contracts c2
    ON c2.contract_address = p.token2
    LEFT JOIN contracts c3
    ON c3.contract_address = p.token3
    LEFT JOIN contracts c4
    ON c4.contract_address = p.token4
    LEFT JOIN contracts c5
    ON c5.contract_address = p.token5
    LEFT JOIN contracts c6
    ON c6.contract_address = p.token6
    LEFT JOIN contracts c7
    ON c7.contract_address = p.token7
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
        t1.decimals :token0 :: INT IS NULL
        AND t1._inserted_timestamp < (
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
            contracts C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.tokens :token0 :: STRING)
          GROUP BY
            1
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
            t2.decimals :token1 :: INT IS NULL
            AND t2._inserted_timestamp < (
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
                contracts C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.tokens :token1 :: STRING)
              GROUP BY
                1
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
                t3.decimals :token2 :: INT IS NULL
                AND t3._inserted_timestamp < (
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
                    contracts C
                  WHERE
                    C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND C.token_decimals IS NOT NULL
                    AND C.contract_address = t3.tokens :token2 :: STRING)
                  GROUP BY
                    1
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
                    t4.decimals :token3 :: INT IS NULL
                    AND t4._inserted_timestamp < (
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
                        contracts C
                      WHERE
                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.token_decimals IS NOT NULL
                        AND C.contract_address = t4.tokens :token3 :: STRING)
                      GROUP BY
                        1
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
                        t5.decimals :token4 :: INT IS NULL
                        AND t5._inserted_timestamp < (
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
                            contracts C
                          WHERE
                            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                            AND C.token_decimals IS NOT NULL
                            AND C.contract_address = t5.tokens :token4 :: STRING)
                          GROUP BY
                            1
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
                            t6.decimals :token5 :: INT IS NULL
                            AND t6._inserted_timestamp < (
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
                                contracts C
                              WHERE
                                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                AND C.token_decimals IS NOT NULL
                                AND C.contract_address = t6.tokens :token5 :: STRING)
                              GROUP BY
                                1
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
                                t7.decimals :token6 :: INT IS NULL
                                AND t7._inserted_timestamp < (
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
                                    contracts C
                                  WHERE
                                    C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                    AND C.token_decimals IS NOT NULL
                                    AND C.contract_address = t7.tokens :token6 :: STRING)
                                  GROUP BY
                                    1
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
                                    t8.decimals :token7 :: INT IS NULL
                                    AND t8._inserted_timestamp < (
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
                                        contracts C
                                      WHERE
                                        C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                        AND C.token_decimals IS NOT NULL
                                        AND C.contract_address = t8.tokens :token7 :: STRING)
                                      GROUP BY
                                        1
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
