{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('streamline__contract_reads_records') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = 'complete_tvl_id',
  post_hook = '{{ unverify_tvl() }}',
  tags = ['silver','defi','tvl','complete','heal','curated_daily']
) }}

{% set models = [
    (ref('silver_tvl__aave_v1_tvl'), 12, 'aave-v1'),
    (ref('silver_tvl__aave_v2_tvl'), 12, 'aave-v2'),
    (ref('silver_tvl__aave_v3_tvl'), 12, 'aave-v3'),
    (ref('silver_tvl__aerodrome_v1_tvl'), 12, 'aerodrome-v1'),
    (ref('silver_tvl__superchain_slipstream_v1_tvl'), 12, 'superchain-slipstream-v1'),
    (ref('silver_tvl__binance_v1_tvl'), 12, 'binance-v1'),
    (ref('silver_tvl__curve_v1_tvl'), 10, 'curve-v1'),
    (ref('silver_tvl__eigenlayer_v1_tvl'), 12, 'eigenlayer-v1'),
    (ref('silver_tvl__ethena_v1_tvl'), 12, 'ethena-v1'),
    (ref('silver_tvl__etherfi_v1_tvl_agg'), 12, 'etherfi-v1'),
    (ref('silver_tvl__lido_v1_tvl'), 12, 'lido-v1'),
    (ref('silver_tvl__rocketpool_v1_tvl'), 12, 'rocketpool-v1'),
    (ref('silver_tvl__sky_v1_tvl'), 12, 'sky-v1'),
    (ref('silver_tvl__tornado_cash_v1_tvl'), 9, 'tornado_cash-v1'),
    (ref('silver_tvl__uniswap_v1_tvl'), 9, 'uniswap-v1'),
    (ref('silver_tvl__uniswap_v2_tvl'), 9, 'uniswap-v2'),
    (ref('silver_tvl__uniswap_v3_tvl'), 9, 'uniswap-v3'),
    (ref('silver_tvl__uniswap_v4_tvl'), 9, 'uniswap-v4'),
    (ref('silver_tvl__morpho_blue_v1_tvl'), 12, 'morpho-v1'),
    (ref('silver_tvl__compound_v1_tvl'), 12, 'compound-v1'),
    (ref('silver_tvl__compound_v2_tvl'), 12, 'compound-v2'),
    (ref('silver_tvl__compound_v3_tvl'), 12, 'compound-v3')
] %}

WITH all_tvl AS (
    {% for model, max_usd_exponent, platform in models %}
        SELECT
            block_number,
            block_date,
            contract_address,
            address,
            token_address,
            amount_hex,
            amount_raw,
            protocol,
            version,
            platform,
            {{ max_usd_exponent }} AS max_usd_exponent
        FROM {{ model }}
        {% if is_incremental() and platform not in vars.CURATED_FR_MODELS %}
        WHERE modified_timestamp > (
            SELECT MAX(modified_timestamp)
            FROM {{ this }}
        )
        {% endif %}
        {% if not loop.last %}
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
    A.token_address,
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
    A.platform,
    A.max_usd_exponent
  FROM
    all_tvl A
    LEFT JOIN contracts c1
    ON A.token_address = c1.contract_address
    LEFT JOIN prices p1
    ON A.token_address = p1.token_address
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
    t.token_address,
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
    t.platform,
    t.max_usd_exponent
  FROM
    {{ this }}
    t
    LEFT JOIN contracts c1
    ON t.token_address = c1.contract_address
    LEFT JOIN prices p1
    ON t.token_address = p1.token_address
    AND DATEADD(
      'hour',
      23,
      t.block_date
    ) = p1.hour
  WHERE t.decimals IS NULL
    AND c1.token_decimals IS NOT NULL  -- only heal if contract data now exists
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
  token_address,
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
  platform,
  max_usd_exponent
FROM
  heal_model
{% endif %}
)
SELECT
  block_number,
  block_date,
  contract_address,
  address,
  token_address,
  decimals,
  symbol,
  is_verified,
  amount_hex,
  amount_raw,
  amount_precise,
  amount,
  amount_usd,
  protocol,
  version,
  platform,
  max_usd_exponent,
  {{ dbt_utils.generate_surrogate_key(
    ['block_number','block_date','contract_address','address','platform']
  ) }} AS complete_tvl_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
