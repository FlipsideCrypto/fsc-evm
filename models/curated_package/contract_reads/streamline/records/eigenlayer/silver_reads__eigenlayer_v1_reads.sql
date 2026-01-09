{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'eigenlayer_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH strategy_events AS (
    SELECT DISTINCT
        decoded_log :strategy :: STRING AS strategy_address
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x858646372cc42e1a627fce94aa7a7033e7cf075a') -- StrategyManager
        AND event_name = 'StrategyAddedToDepositWhitelist'
        AND block_number >= 17445564 -- Contract deployment block
        AND strategy_address <> LOWER('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7') -- Exclude bEIGEN strategy as per adapter (staking, not TVL)
    {% if is_incremental() %}
    AND modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
strategies AS (
    -- totalShares() reads
    SELECT
        strategy_address AS contract_address,
        'totalShares' AS function_name,
        '0x3a98ef39' AS function_sig,
        RPAD('0x3a98ef39', 64, '0') AS input
    FROM
        strategy_events
    UNION ALL
    -- underlyingToken() reads
    SELECT
        strategy_address AS contract_address,
        'underlyingToken' AS function_name,
        '0x2495a599' AS function_sig,
        RPAD('0x2495a599', 64, '0') AS input
    FROM
        strategy_events
)

SELECT
    contract_address,
    NULL AS address,
    function_name,
    function_sig,
    input,
    NULL :: VARIANT AS metadata,
    'eigenlayer' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS eigenlayer_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM strategies