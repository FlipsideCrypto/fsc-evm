{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'validator_index'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'eigenlayer', 'restaked_native_eth', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Eigenlayer Restaked Native ETH

    Tracks daily native ETH restaked via Eigenlayer pods. Calculates the sum of
    effective balances for validators whose withdrawal addresses point to
    Eigenlayer pod addresses. Only includes active validators.
#}

-- Current validators with their withdrawal address, at the most recent snapshot of the validator table (highest slot number)
WITH ValidatorsCurrent AS (
    SELECT
        index AS validator_index,
        '0x' || RIGHT(withdrawal_credentials, 40) AS withdrawal_address,
        slot_number,
        validator_status,
        effective_balance,
        modified_timestamp,
        DATE_TRUNC('day', inserted_timestamp) AS day_
    FROM {{ ref('beacon__fact_validators') }} v -- flipside table 'ethereum.beacon_chain.fact_validators
    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', inserted_timestamp) >= (SELECT MAX(date) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY index, withdrawal_credentials, day_
        ORDER BY slot_number DESC
    ) = 1
),

-- All eigenlayer pod addresses compiled from eigenlayer pod deployed events
EigenPods AS (
    SELECT
        decoded_log:eigenPod::STRING AS eigenpod_address,
        DATE_TRUNC('day', block_timestamp) AS day_of_pod_deployed_event
    FROM
        {{ ref('core__ez_decoded_event_logs') }}-- flipside table 'ethereum.core.ez_decoded_event_logs'
    WHERE
        contract_address = lower('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
        AND event_name = 'PodDeployed'
),

-- Sum of all effective balance of validators that are restaked on eigenlayer pods
DailyRestakedNativeETH AS (
    SELECT
        v.day_ AS date,
        v.validator_index,
        SUM(v.effective_balance) AS restaked_native_eth,
        'ethereum' AS chain,
        'eigenlayer' AS protocol
    FROM ValidatorsCurrent v
    INNER JOIN EigenPods e
        ON e.eigenpod_address = v.withdrawal_address
        AND e.day_of_pod_deployed_event <= v.day_  -- ensures we only count pods after they're deployed
    WHERE validator_status IN ('active_ongoing') --other statuses: 'exited_unslashed', 'withdrawal_possible', 'pending_queued', 'pending_initialized', 'withdrawal_done'
    GROUP BY v.day_, v.validator_index
    ORDER BY v.day_, v.validator_index
)

SELECT
    date,
    validator_index,
    restaked_native_eth,
    chain,
    protocol,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM DailyRestakedNativeETH
