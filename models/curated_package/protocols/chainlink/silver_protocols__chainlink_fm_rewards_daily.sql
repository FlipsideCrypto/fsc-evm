{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'operator_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'rewards', 'fm', 'curated']
) }}

{#
    Chainlink FM Rewards Daily

    Aggregates daily Flux Monitor rewards by operator.
    FM submissions are identified by topic hashes:
    - 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
    - 0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451

    This model works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.
#}

WITH fm_submissions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topics,
        data,
        origin_from_address,
        modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string IN (
        '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f',
        '0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451'
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
),

token_transfers AS (
    SELECT
        block_timestamp::date AS date,
        from_address,
        to_address,
        amount,
        contract_address AS token_address,
        tx_hash,
        modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE tx_hash IN (SELECT DISTINCT tx_hash FROM fm_submissions)
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
    'FM' AS reward_type,
    to_address AS operator_address,
    token_address,
    SUM(amount) AS reward_amount,
    COUNT(DISTINCT tx_hash) AS tx_count,
    MAX(modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM token_transfers
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
