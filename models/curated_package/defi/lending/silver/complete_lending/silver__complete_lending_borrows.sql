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
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, borrower, protocol_market), SUBSTRING(origin_function_signature, event_name, token_address, token_symbol, borrower, protocol_market)",
    tags = ['silver','defi','lending','curated','heal','complete_lending']
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
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__aave_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'aave' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__euler_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'euler' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__fraxlend_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'fraxlend' not in vars.CURATED_FR_MODELS %}
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
        borrower,
        protocol_market,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__aave_ethereum_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'aave_ethereum' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        protocol_market,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__comp_v2_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'comp_v2_fork' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
compound_v3 AS (
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
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__comp_v3_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'compound_v3' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
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
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        A._LOG_ID,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__silo_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'silo' not in vars.CURATED_FR_MODELS %}
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
        borrower,
        protocol_market,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version :: STRING AS version,
        _id as _log_id,
        A.modified_timestamp,
        A.event_name
    FROM
        {{ ref('silver__morpho_borrows') }} A
    WHERE
        token_symbol IS NOT NULL

{% if is_incremental() and 'morpho' not in vars.CURATED_FR_MODELS %}
  AND A.modified_timestamp >= (
    SELECT
      MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
borrow_union AS (
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
        compound_v3
    UNION ALL
    SELECT
        *
    FROM
        silo
    UNION ALL
    SELECT
        *
    FROM
        morpho
    UNION ALL
    SELECT
        *
    FROM
        fraxlend
    UNION ALL
    SELECT
        *
    FROM
        euler
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
        b.event_name,
        borrower,
        protocol_market,
        b.token_address,
        b.token_symbol,
        amount_unadj,
        amount,
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
        borrow_union b
        LEFT JOIN prices
        p
        ON b.token_address = p.token_address
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
        borrower,
        protocol_market,
        t0.token_address,
        t0.token_symbol,
        amount_unadj,
        amount,
        ROUND(
            amount * p.price,
            2
        ) AS amount_usd_heal,
        platform,
        protocol,
        version :: STRING AS version,
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
        borrower,
        protocol_market,
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
    borrower,
    protocol_market,
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
)
SELECT
    *,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS blockchain,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
