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
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, protocol_market)",
  tags = ['silver','defi','lending','curated','heal','flashloans','complete_lending']
) }}

 WITH prices AS (
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
        protocol_market,
        initiator,
        target,
        token_address,
        token_symbol,
        flashloan_amount_unadj,
        flashloan_amount,
        premium_amount_unadj,
        premium_amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__aave_flashloans') }} A
    WHERE flashloan_amount is not null

{% if is_incremental() and 'aave' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        protocol_market,
        initiator,
        target,
        token_address,
        token_symbol,
        flashloan_amount_unadj,
        flashloan_amount,
        premium_amount_unadj,
        premium_amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__aave_ethereum_flashloans') }} A
    WHERE flashloan_amount is not null

{% if is_incremental() and 'aave_ethereum' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        protocol_market,
        initiator,
        target,
        token_address,
        token_symbol,
        flashloan_amount_unadj,
        flashloan_amount,
        premium_amount_unadj,
        premium_amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._log_id,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__morpho_flashloans') }} A
    WHERE flashloan_amount is not null

{% if is_incremental() and 'morpho' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
flashloans AS (
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
    morpho
),
complete_lending_flashloans AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    f.contract_address,
    'FlashLoan' AS event_name,
    protocol_market,
    initiator,
    target,
    f.token_address,
    f.token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    ROUND(
      flashloan_amount * price,
      2
    ) AS flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    ROUND(
      premium_amount * price,
      2
    ) AS premium_amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    f._LOG_ID,
    f.modified_timestamp
  FROM
    flashloans f
    LEFT JOIN prices
    p
    ON f.token_address = p.token_address
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
    initiator,
    target,
    token_address,
    token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    ROUND(
      flashloan_amount * p.price,
      2
    ) AS flashloan_amount_usd_heal,
    premium_amount_unadj,
    premium_amount,
    ROUND(
      premium_amount * p.price,
      2
    ) AS premium_amount_usd_heal,
    platform,
    protocol,
    version,
    t0._LOG_ID,
    t0.modified_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN prices
    p
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
        t1.flashloan_amount_usd IS NULL
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
    OR CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t2.block_number,
          '-',
          t2.platform
        )
      FROM
        {{ this }}
        t2
      WHERE
        t2.premium_amount_usd IS NULL
        AND t2.modified_timestamp < (
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
            AND p.token_address = t2.token_address
            AND p.hour = DATE_TRUNC(
              'hour',
              t2.block_timestamp
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
    protocol_market,
    initiator,
    target,
    token_address,
    token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    premium_amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    _LOG_ID,
    modified_timestamp AS _inserted_timestamp
  FROM
    complete_lending_flashloans

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
  protocol_market,
  initiator,
  target,
  token_address,
  token_symbol,
  flashloan_amount_unadj,
  flashloan_amount,
  flashloan_amount_usd_heal AS flashloan_amount_usd,
  premium_amount_unadj,
  premium_amount,
  premium_amount_usd_heal AS premium_amount_usd,
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
  '{{ vars.GLOBAL_PROJECT_NAME }}' AS blockchain,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_flashloans_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
      _inserted_timestamp DESC)) = 1
