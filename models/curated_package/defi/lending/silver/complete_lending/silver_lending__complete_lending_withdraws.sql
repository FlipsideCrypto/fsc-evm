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
    post_hook = [
      "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, depositor, protocol_market)",
      "{{ remove_test_tokens() }}"
    ],
    tags = ['silver','defi','lending','curated','heal','complete_lending']
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
        depositor,
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
        {{ ref('silver_lending__aave_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__euler_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__aave_ethereum_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__fraxlend_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__comp_v2_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__comp_v3_withdraws') }} A

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
        depositor,
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
        {{ ref('silver_lending__silo_withdraws') }} A

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
        depositor,
        protocol_market,
        token_address,
        amount_unadj,
        platform,
        protocol,
        version,
        _id as _log_id,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver_lending__morpho_withdraws') }} A

{% if is_incremental() and 'morpho' not in vars.CURATED_FR_MODELS %}
  WHERE A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
withdraws AS (
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
complete_lending_withdraws AS (
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
        protocol_market,
        depositor,
        A.token_address,
        C.token_symbol,
        amount_unadj,
        amount_unadj / pow(10, C.token_decimals) AS amount,
        ROUND(
            amount * price,
            2
        ) AS amount_usd,
        platform,
        protocol,
        version,
        A._log_id,
        A.modified_timestamp
    FROM
        withdraws A
        LEFT JOIN contracts C
        ON A.token_address = C.contract_address
        LEFT JOIN prices
        p
        ON A.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
),

{% if is_incremental() and var(
    'HEAL_MODEL'
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
        t0.tx_hash,
        t0.block_number,
        t0.block_timestamp,
        t0.event_index,
        t0.origin_from_address,
        t0.origin_to_address,
        t0.origin_function_signature,
        t0.contract_address,
        t0.event_name,
        t0.protocol_market,
        t0.depositor,
        t0.token_address,
        COALESCE(NULLIF(t0.token_symbol, ''), tm.underlying_token_symbol, c.token_symbol) as token_symbol,
        t0.amount_unadj,
        COALESCE(t0.amount, t0.amount_unadj / pow(10, tm.underlying_token_decimals), t0.amount_unadj / pow(10, c.token_decimals)) AS amount,
        ROUND(
            COALESCE(t0.amount, t0.amount_unadj / pow(10, tm.underlying_token_decimals), t0.amount_unadj / pow(10, c.token_decimals)) * p.price,
            2
        ) AS amount_usd_heal,
        t0.platform,
        t0.protocol,
        t0.version :: STRING AS version,
        t0._log_id,
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
    depositor,
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
    complete_lending_withdraws

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
    depositor,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd_heal AS amount_usd,
    platform,
    protocol,
    version :: STRING AS version,
    _log_id,
    modified_timestamp AS _inserted_timestamp
FROM
    heal_model
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id']
    ) }} AS complete_lending_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id,platform
ORDER BY
      _inserted_timestamp DESC)) = 1
