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
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, token_address, token_symbol, depositor, protocol_market), SUBSTRING(origin_function_signature, event_name, token_address, token_symbol, depositor, protocol_market)",
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
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        redeemer AS depositor_address,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_v3_fork_withdraws') }} A

{% if is_incremental() and 'aave_v3_fork' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
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
        itoken AS protocol_market,
        received_contract_address AS token_address,
        received_contract_symbol AS token_symbol,
        amount_unadj,
        amount,
        redeemer AS depositor_address,
        platform,
        protocol,
        version,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__comp_v2_fork_withdraws') }} A

{% if is_incremental() and 'comp_v2_fork' not in vars.CURATED_FR_MODELS %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
    FROM
      {{ this }}
  )
{% endif %}
),
withdraws AS (
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
        CASE
            WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
            WHEN platform = 'Lodestar' THEN 'Redeem'
            ELSE 'Withdraw'
        END AS event_name,
        protocol_market,
        depositor_address AS depositor,
        A.token_address,
        A.token_symbol,
        amount_unadj,
        amount,
        ROUND(
            amount * price,
            2
        ) AS amount_usd,
        platform,
        protocol,
        version,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraws A
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
        depositor,
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
        t0._log_id,
        t0._inserted_timestamp
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
                AND t1._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
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
                        p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
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
    version,
    _log_id,
    _inserted_timestamp
FROM
    heal_model
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
