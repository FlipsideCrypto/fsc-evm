{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('silver__complete_token_prices') }}
-- depends_on: {{ ref('silver_lending__token_metadata') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market)",
  tags = ['silver','defi','lending','curated','heal','liquidations','complete_lending']
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
    modified_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS token_address,
    price,
    HOUR,
    is_verified,
    modified_timestamp
  FROM
    {{ ref('price__ez_prices_hourly') }}
  WHERE
    token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
),
aave AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__aave_liquidations') }} A


{% if is_incremental() and 'aave' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
  euler AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__euler_liquidations') }} A

{% if is_incremental() and 'euler' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
aave_ethereum AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        liquidator,
        borrower,
        protocol_market,
        collateral_token,
        liquidated_amount_unadj,
        debt_token,
        repaid_amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__aave_ethereum_liquidations') }} A

{% if is_incremental() and 'aave_ethereum' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
fraxlend AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        liquidator,
        borrower,
        protocol_market,
        collateral_token,
        liquidated_amount_unadj,
        debt_token,
        repaid_amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__fraxlend_liquidations') }} A

{% if is_incremental() and 'fraxlend' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
comp_v2_fork AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__comp_v2_liquidations') }} A

{% if is_incremental() and 'comp_v2_fork' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
comp_v3 AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__comp_v3_liquidations') }} A

{% if is_incremental() and 'comp_v3' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
silo AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__silo_liquidations') }} A  

{% if is_incremental() and 'silo' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
morpho AS (
    SELECT
      tx_hash,
      block_number,
      block_timestamp,
      null as event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      borrower,
      liquidator,
      protocol_market,
      collateral_token,
      liquidated_amount_unadj,
      debt_token,
      repaid_amount_unadj,
      platform,
      protocol,
      version :: STRING AS version,
      _id as _log_id,
      modified_timestamp,
      event_name
    FROM
        {{ ref('silver_lending__morpho_liquidations') }} A

{% if is_incremental() and 'morpho' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union AS (
  SELECT
    *
  FROM
    aave
  UNION ALL
  SELECT
    *
  FROM
    aave_ethereum
  UNION ALL
  SELECT
    *
  FROM
    comp_v2_fork
  UNION ALL
  SELECT
    *
  FROM
    comp_v3
  UNION ALL
  SELECT
    *
  FROM
    silo
  UNION ALL
  SELECT
    *
  FROM
    fraxlend
  UNION ALL
  SELECT
    *
  FROM
    morpho
  UNION ALL
  SELECT
    *
  FROM
    euler
),

complete_lending_liquidations AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    A.event_name,
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    C.token_symbol AS collateral_token_symbol,
    liquidated_amount_unadj,
    liquidated_amount_unadj / pow(10, C.token_decimals) AS liquidated_amount,
    ROUND(liquidated_amount * p1.price, 2) AS liquidated_amount_usd,
    debt_token,
    C2.token_symbol AS debt_token_symbol,
    repaid_amount_unadj,
    repaid_amount_unadj / pow(10, C2.token_decimals) AS repaid_amount,
    ROUND(repaid_amount * p2.price, 2) AS repaid_amount_usd,
    platform,
    protocol,
    version,
    A._LOG_ID,
    A.modified_timestamp
  FROM
    liquidation_union A
    LEFT JOIN contracts C
    ON A.collateral_token = C.contract_address
    LEFT JOIN contracts C2
    ON A.debt_token = C2.contract_address
    LEFT JOIN prices
    p1
    ON collateral_token = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices
    p2
    ON debt_token = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
token_metadata AS (
select 
    underlying_token_address,
    underlying_token_symbol,
    underlying_token_decimals
 from 
  {{ ref('silver_lending__token_metadata') }}
),
heal_model AS (
  SELECT
    t0.tx_hash,
    t0.block_number,
    t0.block_timestamp,
    t0.event_index,
    t0.origin_from_address,
    t0.origin_to_address,
    t0.origin_function_signature,
    t0.contract_address,
    t0.event_name,
    t0.liquidator,
    t0.borrower,
    t0.protocol_market,
    t0.collateral_token,
    COALESCE(NULLIF(t0.collateral_token_symbol, ''), tm1.underlying_token_symbol, c1.token_symbol) as collateral_token_symbol,
    t0.liquidated_amount_unadj,
    COALESCE(t0.liquidated_amount, t0.liquidated_amount_unadj / pow(10, tm1.underlying_token_decimals), t0.liquidated_amount_unadj / pow(10, c1.token_decimals)) AS liquidated_amount,
    ROUND(
      COALESCE(t0.liquidated_amount, t0.liquidated_amount_unadj / pow(10, tm1.underlying_token_decimals), t0.liquidated_amount_unadj / pow(10, c1.token_decimals)) * p1.price,
      2
    ) AS liquidated_amount_usd_heal,
    t0.debt_token,
    COALESCE(NULLIF(t0.debt_token_symbol, ''), tm2.underlying_token_symbol, c2.token_symbol) as debt_token_symbol,
    t0.repaid_amount_unadj,
    COALESCE(t0.repaid_amount, t0.repaid_amount_unadj / pow(10, tm2.underlying_token_decimals), t0.repaid_amount_unadj / pow(10, c2.token_decimals)) AS repaid_amount,
    ROUND(
      COALESCE(t0.repaid_amount, t0.repaid_amount_unadj / pow(10, tm2.underlying_token_decimals), t0.repaid_amount_unadj / pow(10, c2.token_decimals)) * p2.price,
      2
    ) AS repaid_amount_usd_heal,
    t0.platform,
    t0.protocol,
    t0.version :: STRING AS version,
    t0._LOG_ID,
    t0.modified_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN token_metadata tm1
    ON t0.collateral_token = tm1.underlying_token_address
    LEFT JOIN token_metadata tm2
    ON t0.debt_token = tm2.underlying_token_address
    LEFT JOIN contracts c1
    ON t0.collateral_token = c1.contract_address
    LEFT JOIN contracts c2
    ON t0.debt_token = c2.contract_address
    LEFT JOIN prices p1
    ON t0.collateral_token = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON t0.debt_token = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.liquidated_amount_usd IS NULL
        AND t1.modified_timestamp < (
          SELECT
            MAX(
              modified_timestamp
            ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('price__ez_prices_hourly') }}
            p1
          WHERE
            p1.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p1.price IS NOT NULL
            AND p1.token_address = t1.collateral_token
            AND p1.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('price__ez_prices_hourly') }}
            p2
          WHERE
            p2.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p2.price IS NOT NULL
            AND p2.token_address = t1.debt_token
            AND p2.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
    OR (t0.collateral_token_symbol IS NULL OR t0.collateral_token_symbol = '' AND tm1.underlying_token_symbol IS NOT NULL AND tm1.underlying_token_symbol != '')
    OR (t0.collateral_token_symbol IS NULL OR t0.collateral_token_symbol = '' AND c1.token_symbol IS NOT NULL AND c1.token_symbol != '')
    OR (t0.debt_token_symbol IS NULL OR t0.debt_token_symbol = '' AND tm2.underlying_token_symbol IS NOT NULL AND tm2.underlying_token_symbol != '')
    OR (t0.debt_token_symbol IS NULL OR t0.debt_token_symbol = '' AND c2.token_symbol IS NOT NULL AND c2.token_symbol != '')
    OR (t0.liquidated_amount IS NULL AND tm1.underlying_token_decimals IS NOT NULL)
    OR (t0.liquidated_amount IS NULL AND c1.token_decimals IS NOT NULL)
    OR (t0.repaid_amount IS NULL AND tm2.underlying_token_decimals IS NOT NULL)
    OR (t0.repaid_amount IS NULL AND c2.token_decimals IS NOT NULL)
),
{% endif %}

FINAL AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    event_name,
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    liquidated_amount_unadj,
    liquidated_amount,
    liquidated_amount_usd,
    debt_token,
    debt_token_symbol,
    repaid_amount_unadj,
    repaid_amount,
    repaid_amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    _LOG_ID,
    modified_timestamp AS _inserted_timestamp
  FROM
    complete_lending_liquidations

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  event_name,
  liquidator,
  borrower,
  protocol_market,
  collateral_token,
  collateral_token_symbol,
  liquidated_amount_unadj,
  liquidated_amount,
  liquidated_amount_usd_heal AS liquidated_amount_usd,
  debt_token,
  debt_token_symbol,
  repaid_amount_unadj,
  repaid_amount,
  repaid_amount_usd_heal AS repaid_amount_usd,
  platform,
  protocol,
  version :: STRING AS version,
  _LOG_ID,
  modified_timestamp AS _inserted_timestamp
FROM
  heal_model
{% endif %}

)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['_log_id']
  ) }} AS complete_lending_liquidations_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
      _inserted_timestamp DESC)) = 1
