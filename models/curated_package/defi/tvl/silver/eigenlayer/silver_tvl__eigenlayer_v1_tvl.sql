{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'eigenlayer_v1_tvl_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

-- EigenLayer TVL: EigenPod (native ETH via beacon chain) + Strategy (LST/ERC20) TVL

-- =====================================================================
-- EIGENPOD TVL: Native ETH Restaking via Beacon Chain Validators
-- =====================================================================

WITH eigenpods AS (
    SELECT DISTINCT
        LOWER(decoded_log:eigenPod::STRING) AS eigenpod_address
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('0x91e677b07f7af907ec9a428aafa9fc14a0d3a338')
        AND event_name = 'PodDeployed'
        AND block_number >= 17445564
),

beacon_blocks AS (
    SELECT
        slot_number,
        slot_timestamp::DATE AS block_date,
        execution_payload:block_number::INT AS block_number,
        modified_timestamp
    FROM {{ source('ethereum_beacon_chain', 'fact_blocks') }}
    WHERE block_included = TRUE
{% if is_incremental() %}
        AND modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }} WHERE component = 'eigenpod')
{% endif %}
),

eigenpod_validators AS (
    SELECT
        v.slot_number,
        LOWER('0x' || RIGHT(v.withdrawal_credentials, 40)) AS eigenpod_address,
        v.balance
    FROM {{ source('ethereum_beacon_chain', 'fact_validators') }} v
    WHERE LEFT(v.withdrawal_credentials, 4) = '0x01'
        AND v.validator_status IN ('active_ongoing', 'pending_queued', 'pending_initialized', 'withdrawal_possible')
{% if is_incremental() %}
        AND v.modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }} WHERE component = 'eigenpod')
{% endif %}
),

eigenpod_tvl AS (
    SELECT
        b.block_date,
        b.block_number,
        ep.eigenpod_address AS contract_address,
        NULL AS address,
        '0x0000000000000000000000000000000000000000' AS token_address,
        NULL AS amount_hex,
        SUM(ev.balance) * POW(10, 18) AS amount_raw,
        'eigenpod' AS component,
        MAX(b.modified_timestamp) AS _modified_timestamp
    FROM eigenpods ep
    INNER JOIN eigenpod_validators ev
        ON ep.eigenpod_address = ev.eigenpod_address
    INNER JOIN beacon_blocks b
        ON ev.slot_number = b.slot_number
    GROUP BY 1, 2, 3
    -- Take latest slot per day per eigenpod
    QUALIFY ROW_NUMBER() OVER (PARTITION BY block_date, contract_address ORDER BY _modified_timestamp DESC) = 1
),

-- =====================================================================
-- STRATEGY TVL: LST/ERC20 Token Restaking
-- =====================================================================

strategy_tvl AS (
    SELECT
        s.block_date,
        s.block_number,
        s.contract_address,
        NULL AS address,
        LOWER('0x' || RIGHT(LTRIM(t.result_hex, '0x'), 40)) AS token_address,
        NULL AS amount_hex,
        IFNULL(
            CASE WHEN LENGTH(s.result_hex) <= 4300 AND s.result_hex IS NOT NULL
                 THEN TRY_CAST(utils.udf_hex_to_int(s.result_hex) AS BIGINT) END,
            CASE WHEN s.result_hex IS NOT NULL
                 THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(s.result_hex, '0')) AS BIGINT) END
        ) AS amount_raw,
        'strategy' AS component
    FROM {{ ref('silver__contract_reads') }} s
    LEFT JOIN {{ ref('silver__contract_reads') }} t
        ON s.contract_address = t.contract_address
        AND s.block_date = t.block_date
        AND t.platform = 'eigenlayer-v1'
        AND t.function_name = 'underlyingToken'
        AND t.result_hex IS NOT NULL
        AND LENGTH(t.result_hex) >= 42
    WHERE s.platform = 'eigenlayer-v1'
        AND s.function_name = 'totalShares'
        AND s.result_hex IS NOT NULL
{% if is_incremental() %}
        AND s.modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }} WHERE component = 'strategy')
{% endif %}
),

-- =====================================================================
-- COMBINE ALL TVL COMPONENTS
-- =====================================================================

combined_tvl AS (
    SELECT block_date, block_number, contract_address, address, token_address, amount_hex, amount_raw, component
    FROM eigenpod_tvl
    UNION ALL
    SELECT block_date, block_number, contract_address, address, token_address, amount_hex, amount_raw, component
    FROM strategy_tvl
)

SELECT
    ct.block_number,
    ct.block_date,
    ct.contract_address,
    ct.address,
    ct.token_address,
    ct.amount_hex,
    ct.amount_raw,
    'eigenlayer' AS protocol,
    'v1' AS version,
    'eigenlayer-v1' AS platform,
    ct.component,
    {{ dbt_utils.generate_surrogate_key(['ct.block_date','ct.contract_address','ct.token_address']) }} AS eigenlayer_v1_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM combined_tvl ct
WHERE ct.amount_raw IS NOT NULL AND ct.amount_raw > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY eigenlayer_v1_tvl_id ORDER BY ct.block_date DESC) = 1
