{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'operator_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'rewards', 'direct', 'curated']
) }}

{#
    Chainlink Direct Rewards Daily

    Aggregates daily direct operator rewards.
    Direct payments are identified by function signatures:
    - 0x4ab0d190
    - 0xfbcafdc9
    - 0xf75f0e7a

    This model works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.
#}

WITH operator_payments AS (
    SELECT
        block_timestamp::date AS date,
        from_address,
        to_address,
        amount,
        contract_address AS token_address,
        tx_hash,
        modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE origin_function_signature IN (
        '0x4ab0d190',
        '0xfbcafdc9',
        '0xf75f0e7a'
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    'Direct' AS reward_type,
    to_address AS operator_address,
    token_address,
    SUM(amount) AS reward_amount,
    COUNT(DISTINCT tx_hash) AS tx_count,
    MAX(modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM operator_payments
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
