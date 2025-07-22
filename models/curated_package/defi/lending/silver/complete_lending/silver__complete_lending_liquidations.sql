{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market), SUBSTRING(origin_function_signature, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market)",
  tags = ['silver','defi','lending','curated','heal']
) }}

WITH contracts AS (

  SELECT
    address AS contract_address,
    symbol AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp
  FROM
    {{ ref('core__dim_contracts') }}
  UNION ALL
  SELECT
    '0x0000000000000000000000000000000000000000' AS contract_address,
    '{{ vars.GLOBAL_NATIVE_ASSET_SYMBOL }}' AS token_symbol,
    decimals AS token_decimals,
    modified_timestamp
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
aave_v3_fork AS (

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
        amount_unadj,
        amount AS liquidated_amount,
        NULL AS liquidated_amount_usd,
        collateral_token AS protocol_collateral_asset,
        collateral_asset,
        collateral_token_symbol AS collateral_asset_symbol,
        debt_asset,
        debt_token_symbol AS debt_asset_symbol,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A.modified_timestamp
    FROM
        {{ ref('silver__aave_v3_fork_liquidations') }} A

{% if is_incremental() and 'aave_v3_fork' not in vars.CURATED_FR_MODELS %}
WHERE
  modified_timestamp >= (
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
        liquidator,
        borrower,
        amount_unadj,
        amount AS liquidated_amount,
        NULL AS liquidated_amount_usd,
        protocol_market AS protocol_collateral_asset,
        collateral_token AS collateral_asset,
        collateral_token_symbol AS collateral_asset_symbol,
        debt_asset,
        debt_asset_symbol,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A.modified_timestamp
    FROM
        {{ ref('silver__comp_v2_fork_liquidations') }} A

{% if is_incremental() and 'comp_v2_fork' not in vars.CURATED_FR_MODELS %}
WHERE
  modified_timestamp >= (
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
    aave_v3_fork
  UNION ALL
  SELECT
    *
  FROM
    comp_v2_fork
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
    CASE
      WHEN platform = 'compound_v3' THEN 'AbsorbCollateral'
      WHEN platform = 'lodestar' THEN 'LiquidateBorrow'
      WHEN platform = 'silo' THEN 'Liquidate'
      ELSE 'LiquidationCall'
    END AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset AS protocol_market,
    collateral_asset AS collateral_token,
    collateral_asset_symbol AS collateral_token_symbol,
    amount_unadj,
    liquidated_amount AS amount,
    CASE
      WHEN platform <> 'compound_v3' THEN ROUND(
        liquidated_amount * p.price,
        2
      )
      ELSE ROUND(
        liquidated_amount_usd,
        2
      )
    END AS amount_usd,
    debt_asset AS debt_token,
    debt_asset_symbol AS debt_token_symbol,
    platform,
    protocol,
    version,
            A._LOG_ID,
        A.modified_timestamp
  FROM
    liquidation_union A
    LEFT JOIN prices
    p
    ON collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
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
    t0.collateral_token_symbol,
    t0.amount_unadj,
    t0.amount,
    CASE
      WHEN t0.platform <> 'compound_v3' THEN ROUND(
        t0.amount * p.price,
        2
      )
      ELSE ROUND(
        t0.amount_usd,
        2
      )
    END AS amount_usd_heal,
    t0.debt_token,
    t0.debt_token_symbol,
    t0.platform,
    t0.protocol,
    t0.version,
    t0._LOG_ID,
    t0.modified_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN prices
    p
    ON t0.collateral_token = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
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
        t1.amount_usd IS NULL
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
            {{ ref('silver__complete_token_prices') }}
            p
          WHERE
            p.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p.price IS NOT NULL
            AND p.token_address = t1.collateral_token
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
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
    amount_unadj,
    amount,
    amount_usd,
    debt_token,
    debt_token_symbol,
    platform,
    protocol,
    version,
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
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  debt_token,
  debt_token_symbol,
  platform,
  protocol,
  version,
  _LOG_ID,
      modified_timestamp AS _inserted_timestamp
FROM
  heal_model
{% endif %}
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_liquidations_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
      _inserted_timestamp DESC)) = 1
