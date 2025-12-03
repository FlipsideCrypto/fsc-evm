{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_tvl_id',
    tags = ['silver','defi','tvl','complete','curated_daily']
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
aave_v1 AS (
    
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
FROM
    {{ ref('silver__aave_v1_tvl') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
aave_v2 AS (
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
FROM
    {{ ref('silver__aave_v2_tvl') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
aave_v3 AS (
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
FROM
    {{ ref('silver__aave_v3_tvl') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
all_tvl AS (
    SELECT
        *
    FROM
        aave_v1
    UNION ALL
    SELECT
        *
    FROM
        aave_v2 
    UNION ALL
    SELECT
        *
    FROM
        aave_v3
),
final AS (
    SELECT
        block_number,
        block_date,
        contract_address,
        address,
        c1.token_decimals AS decimals,
        c1.token_symbol AS symbol,
        amount_hex,
        amount_raw,
        utils.udf_decimal_adjust(
            amount_raw,
            decimals
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        ROUND(amount * p1.price, 2) AS amount_usd,
        protocol,
        version,
        platform
    FROM
        all_tvl a 
    LEFT JOIN contracts
    c1
    ON a.contract_address = c1.contract_address
    LEFT JOIN prices
    p1
    ON a.contract_address = p1.token_address
    AND DATEADD(
      'hour',
      23,
      a.block_date
    ) = p1.hour
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
    final