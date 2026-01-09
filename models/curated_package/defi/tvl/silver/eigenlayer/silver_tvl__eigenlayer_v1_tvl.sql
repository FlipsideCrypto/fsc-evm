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

WITH beacon_blocks AS (
    SELECT
        slot_number,
        slot_timestamp::DATE AS block_date,
        execution_payload:block_number::INT AS block_number,
        modified_timestamp
    FROM {{ source('ethereum_beacon_chain', 'fact_blocks') }}
    WHERE block_included = TRUE
    AND block_date >= ('{{ vars.CURATED_SL_CONTRACT_READS_START_DATE }}' :: TIMESTAMP) :: DATE
    AND block_number IS NOT NULL
{% if is_incremental() %}
        AND modified_timestamp >= (SELECT MAX(modified_timestamp) - INTERVAL '24 hours' FROM {{ this }} WHERE component = 'eigenpod')
{% endif %}
),

eigenpod_validators AS (
    SELECT
        slot_number,
        LOWER('0x' || RIGHT(withdrawal_credentials, 40)) AS eigenpod_address,
        balance
    FROM {{ source('ethereum_beacon_chain', 'fact_validators') }}
    WHERE LEFT(withdrawal_credentials, 4) = '0x01' -- Execution layer withdrawal credentials (funds go to an Ethereum address)
        AND validator_status IN ('active_ongoing', 'pending_queued', 'pending_initialized', 'withdrawal_possible')
        AND slot_number >= (SELECT MIN(slot_number) FROM beacon_blocks)
        AND slot_number IS NOT NULL
{% if is_incremental() %}
        AND modified_timestamp >= (SELECT MAX(modified_timestamp) - INTERVAL '24 hours' FROM {{ this }} WHERE component = 'eigenpod')
{% endif %}
),

eigenpod_tvl AS (
    -- Native ETH Restaking via Beacon Chain Validators
    SELECT
        b.block_date,
        b.block_number,
        ep.eigenpod_address AS contract_address,
        NULL AS address,
        '0x0000000000000000000000000000000000000000' AS token_address, -- represents native ETH, for pricing purposes
        NULL AS amount_hex,
        SUM(ev.balance) * POW(10, 18) AS amount_raw,
        'eigenpod' AS component,
        MAX(b.modified_timestamp) AS _modified_timestamp
    FROM {{ ref('silver_tvl__eigenlayer_v1_eigenpods') }} ep
    INNER JOIN eigenpod_validators ev
        ON ep.eigenpod_address = ev.eigenpod_address
    INNER JOIN beacon_blocks b
        ON ev.slot_number = b.slot_number
    GROUP BY 1, 2, 3
    -- Take latest slot per day per eigenpod
    QUALIFY ROW_NUMBER() OVER (PARTITION BY block_date, contract_address ORDER BY _modified_timestamp DESC) = 1
),

strategy_tvl AS (
    -- LST/ERC20 Token Restaking
    SELECT
        s.block_date,
        s.block_number,
        s.contract_address,
        NULL AS address,
        LOWER('0x' || RIGHT(LTRIM(t.result_hex, '0x'), 40)) AS token_address,
        s.result_hex AS amount_hex,
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
