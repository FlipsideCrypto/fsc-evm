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
    tags = ['silver','defi','lending','curated','heal']
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
        borrower_address AS borrower,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_v3_fork_borrows') }} A

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
        borrower,
        itoken AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        loan_amount AS amount,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__comp_v2_fork_borrows') }} A

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
borrow_union AS (
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
        CASE
            WHEN platform = 'compound_v3' THEN 'Withdraw'
            ELSE 'Borrow'
        END AS event_name,
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
        version,
        b._LOG_ID,
        b._INSERTED_TIMESTAMP
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
        version,
        t0._LOG_ID,
        t0._INSERTED_TIMESTAMP
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
        *
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
    version,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    heal_model
{% endif %}
)
SELECT
    *,
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
