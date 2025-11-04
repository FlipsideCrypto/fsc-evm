  -- depends_on: {{ ref('silver_lending__token_metadata') }}
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
    on_schema_change = 'append_new_columns',
    contract = {
        "enforced": False,
        "warn_unsupported": True
    },
    post_hook = [
      "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, borrower, protocol_market)"
    ],
    tags = ['silver','defi','lending','curated','heal','complete_lending','complete_borrows']
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
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__aave_borrows') }} A

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
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__euler_borrows') }} A

{% if is_incremental() and 'euler' not in vars.CURATED_FR_MODELS %}
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
        borrower,
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__fraxlend_borrows') }} A

{% if is_incremental() and 'fraxlend' not in vars.CURATED_FR_MODELS %}
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
        NULL AS event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower,
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._id AS _LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__morpho_borrows') }} A

{% if is_incremental() and 'morpho' not in vars.CURATED_FR_MODELS %}
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
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__silo_borrows') }} A

{% if is_incremental() and 'silo' not in vars.CURATED_FR_MODELS %}
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
        borrower,
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__aave_ethereum_borrows') }} A

{% if is_incremental() and 'aave_ethereum' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
comp_v2 AS (

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
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__comp_v2_borrows') }} A

{% if is_incremental() and 'comp_v2' not in vars.CURATED_FR_MODELS %}
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
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__comp_v3_borrows') }} A

{% if is_incremental() and 'comp_v3' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
borrows AS (
  SELECT
    *
  FROM
    aave
  UNION ALL
  SELECT
    *
  FROM
    euler
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
    silo
  UNION ALL
  SELECT
    *
  FROM
    aave_ethereum
  UNION ALL
  SELECT
    *
  FROM
    comp_v2
  UNION ALL
  SELECT
    *
  FROM
    comp_v3
),

complete_lending_borrows AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    b.contract_address,
    event_name,
    protocol_market,
    borrower,
    b.token_address,
    C.token_symbol,
    amount_unadj,
    amount_unadj / pow(10, C.token_decimals) AS amount,
    ROUND(
      amount * price,
      2
    ) AS amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    b._LOG_ID,
    b.modified_timestamp
  FROM
    borrows b
    LEFT JOIN contracts C
    ON b.token_address = C.contract_address
    LEFT JOIN prices
    p
    ON b.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
),


{% if is_incremental() and var(
  'HEAL_MODEL', false
) %}
token_metadata AS (
  SELECT
      underlying_token_address,
      underlying_token_symbol,
      underlying_token_decimals
  FROM
    {{ ref('silver_lending__token_metadata') }}
  WHERE 
    blockchain = '{{ vars.GLOBAL_PROJECT_NAME }}'
),
heal_model AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t0.contract_address,
    event_name,
    protocol_market,
    borrower,
    t0.token_address,
    COALESCE(NULLIF(t0.token_symbol, ''), tm.underlying_token_symbol, c.token_symbol) as token_symbol,
    amount_unadj,
    COALESCE(t0.amount, amount_unadj / pow(10, tm.underlying_token_decimals), amount_unadj / pow(10, c.token_decimals)) AS amount,
    ROUND(
      COALESCE(t0.amount, amount_unadj / pow(10, tm.underlying_token_decimals), amount_unadj / pow(10, c.token_decimals)) * p.price,
      2
    ) AS amount_usd_heal,
    platform,
    protocol,
    version,
    t0._LOG_ID,
    t0.modified_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN token_metadata tm
    ON t0.token_address = tm.underlying_token_address
    LEFT JOIN contracts c
    ON t0.token_address = c.contract_address
    LEFT JOIN prices p
    ON t0.token_address = p.token_address
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
            AND p.token_address = t1.token_address
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
    OR (t0.token_symbol IS NULL OR t0.token_symbol = '' AND tm.underlying_token_symbol IS NOT NULL AND tm.underlying_token_symbol != '')
    OR (t0.token_symbol IS NULL OR t0.token_symbol = '' AND c.token_symbol IS NOT NULL AND c.token_symbol != '')
    OR (t0.amount IS NULL AND tm.underlying_token_decimals IS NOT NULL)
    OR (t0.amount IS NULL AND c.token_decimals IS NOT NULL)
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
    protocol_market,
    borrower,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    _LOG_ID,
    modified_timestamp AS _inserted_timestamp
  FROM
    complete_lending_borrows

{% if is_incremental() and var(
  'HEAL_MODEL', false
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
  protocol_market,
  borrower,
  token_address,
  token_symbol,
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  platform,
  protocol,
  version :: STRING AS version,
  _LOG_ID,
  modified_timestamp AS _inserted_timestamp
FROM
  heal_model
{% endif %}

),

{% if var('test_constraint_with_null', false) %}
test_data_with_null AS (
  SELECT
    '0x' || REPEAT('a', 64) AS tx_hash,
    999999999 AS block_number,
    CURRENT_TIMESTAMP() AS block_timestamp,
    0 AS event_index,
    '0x' || REPEAT('b', 40) AS origin_from_address,
    '0x' || REPEAT('c', 40) AS origin_to_address,
    '0x' || REPEAT('d', 8) AS origin_function_signature,
    '0x' || REPEAT('e', 40) AS contract_address,
    'Borrow' AS event_name,
    '0x' || REPEAT('f', 40) AS protocol_market,
    '0x' || REPEAT('g', 40) AS borrower,
    '0x' || REPEAT('h', 40) AS token_address,
    NULL AS token_symbol,  -- This NULL should trigger constraint violation
    1000000 AS amount_unadj,
    1.0 AS amount,
    100.50 AS amount_usd,
    'test_platform' AS platform,
    'test_protocol' AS protocol,
    'v1' AS version,
    'test_log_id_12369' AS _log_id,
    CURRENT_TIMESTAMP() AS _inserted_timestamp
),
{% endif %}

FINAL_WITH_TEST AS (
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
    protocol_market,
    borrower,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    platform,
    protocol,
    version,
    _LOG_ID,
    _inserted_timestamp
  FROM
    FINAL
  {% if var('test_constraint_with_null', false) %}
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
    protocol_market,
    borrower,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    platform,
    protocol,
    version,
    _LOG_ID,
    _inserted_timestamp
  FROM
    test_data_with_null
  {% endif %}
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['_log_id']
  ) }} AS complete_lending_borrows_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL_WITH_TEST qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
      _inserted_timestamp DESC)) = 1
