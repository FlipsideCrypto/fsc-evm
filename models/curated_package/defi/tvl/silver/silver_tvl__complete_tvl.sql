{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'complete_tvl_id',
  tags = ['silver','defi','tvl','complete','heal','curated_daily']
) }}

{% set models = [] %}
{% set _ = models.append(ref('silver__aave_v1_tvl')) %}
{% set _ = models.append(ref('silver__aave_v2_tvl')) %}
{% set _ = models.append(ref('silver__aave_v3_tvl')) %}
{% set _ = models.append(ref('silver__curve_v1_tvl')) %}
{% set _ = models.append(ref('silver__lido_v1_tvl')) %}
{% set _ = models.append(ref('silver__tornado_cash_v1_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v1_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v2_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v3_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v4_tvl')) %}

WITH all_tvl AS (
    {% for model in models %}
        SELECT
            block_number,
            block_date,
            contract_address,
            address,
            amount_hex,
            amount_raw,
            protocol,
            version,
            platform
        FROM {{ model }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE modified_timestamp > (
            SELECT MAX(modified_timestamp)
            FROM {{ this }}
        )
        {% endif %}
        UNION ALL
        {% endif %}
    {% endfor %}
),
contracts AS (
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
complete_tvl AS (
  SELECT
    A.block_number,
    A.block_date,
    A.contract_address,
    A.address,
    c1.token_decimals AS decimals,
    c1.token_symbol AS symbol,
    p1.is_verified,
    A.amount_hex,
    A.amount_raw,
    utils.udf_decimal_adjust(
      A.amount_raw,
      c1.token_decimals
    ) AS amount_precise,
    amount_precise :: FLOAT AS amount,
    ROUND(
      amount * p1.price,
      2
    ) AS amount_usd,
    A.protocol,
    A.version,
    A.platform
  FROM
    all_tvl A
    LEFT JOIN contracts c1
    ON A.contract_address = c1.contract_address
    LEFT JOIN prices p1
    ON A.contract_address = p1.token_address
    AND DATEADD(
      'hour',
      23,
      A.block_date
    ) = p1.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t.block_number,
    t.block_date,
    t.contract_address,
    t.address,
    c1.token_decimals AS decimals_heal,
    c1.token_symbol AS symbol_heal,
    p1.is_verified AS is_verified_heal,
    t.amount_hex,
    t.amount_raw,
    utils.udf_decimal_adjust(
      t.amount_raw,
      c1.token_decimals
    ) AS amount_precise_heal,
    amount_precise_heal :: FLOAT AS amount_heal,
    ROUND(
      amount_heal * p1.price,
      2
    ) AS amount_usd_heal,
    t.protocol,
    t.version,
    t.platform
  FROM
    {{ this }}
    t
    LEFT JOIN contracts c1
    ON A.contract_address = c1.contract_address
    LEFT JOIN prices p1
    ON A.contract_address = p1.token_address
    AND DATEADD(
      'hour',
      23,
      A.block_date
    ) = p1.hour
),
{% endif %}

FINAL AS (
  SELECT
    *
  FROM
    complete_tvl

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_date,
  contract_address,
  address,
  decimals_heal AS decimals,
  symbol_heal AS symbol,
  is_verified_heal AS is_verified,
  amount_hex,
  amount_raw,
  amount_precise_heal AS amount_precise,
  amount_heal AS amount,
  amount_usd_heal AS amount_usd,
  protocol,
  version,
  platform
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_date,
  contract_address,
  address,
  decimals,
  symbol,
  amount_hex,
  amount_raw,
  amount_precise,
  amount,
  amount_usd,
  protocol,
  version,
  platform,
  {{ dbt_utils.generate_surrogate_key(
    ['block_date','contract_address','address','platform']
  ) }} AS complete_tvl_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
