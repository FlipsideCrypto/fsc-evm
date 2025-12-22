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
    post_hook = [
        "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, contract_address, token_address, token_symbol, withdrawer, receiver, protocol_market)"
    ],
    tags = ['silver','defi','erc4626','curated','complete_erc4626']
) }}

WITH contracts AS (
    SELECT
        address AS contract_address,
        symbol AS token_symbol,
        decimals AS token_decimals,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__dim_contracts') }}
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
),

maple AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        vault_address AS contract_address,
        withdrawer,
        receiver,
        protocol_market,
        protocol_market_symbol,
        token_address,
        token_symbol,
        token_decimals,
        amount_unadj,
        shares_unadj,
        platform,
        protocol,
        version::STRING AS version,
        _log_id,
        modified_timestamp,
        event_name
    FROM
        {{ ref('silver_erc4626__maple_withdrawals') }}

{% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
        FROM
            {{ this }}
    )
{% endif %}
),

{# Add additional ERC4626 protocols here with UNION ALL #}

withdrawals AS (
    SELECT * FROM maple
),

complete_erc4626_withdrawals AS (
    SELECT
        w.tx_hash,
        w.block_number,
        w.block_timestamp,
        w.event_index,
        w.origin_from_address,
        w.origin_to_address,
        w.origin_function_signature,
        w.contract_address,
        w.event_name,
        w.protocol_market,
        w.protocol_market_symbol,
        w.withdrawer,
        w.receiver,
        w.token_address,
        COALESCE(w.token_symbol, c.token_symbol) AS token_symbol,
        w.amount_unadj,
        w.amount_unadj / pow(10, COALESCE(w.token_decimals, c.token_decimals)) AS amount,
        w.shares_unadj,
        w.shares_unadj / pow(10, 18) AS shares,
        ROUND(
            (w.amount_unadj / pow(10, COALESCE(w.token_decimals, c.token_decimals))) * p.price,
            2
        ) AS amount_usd,
        w.platform,
        w.protocol,
        w.version::STRING AS version,
        w._log_id,
        w.modified_timestamp
    FROM
        withdrawals w
    LEFT JOIN contracts c
        ON w.token_address = c.contract_address
    LEFT JOIN prices p
        ON w.token_address = p.token_address
        AND DATE_TRUNC('hour', w.block_timestamp) = p.hour
),

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
        protocol_market_symbol,
        withdrawer,
        receiver,
        token_address,
        token_symbol,
        amount_unadj,
        amount,
        shares_unadj,
        shares,
        amount_usd,
        platform,
        protocol,
        version::STRING AS version,
        _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        complete_erc4626_withdrawals
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['_log_id']) }} AS complete_erc4626_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
QUALIFY(ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY _inserted_timestamp DESC)) = 1
