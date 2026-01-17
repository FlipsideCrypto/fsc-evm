{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address', 'protocol'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'flashloan_fees', 'curated']
) }}

{#
    Aave Flashloan Fees - Consolidated Cross-Chain Model

    Aggregates flashloan premium fees from Aave V2 and V3 deployments.
    Uses Flipside's ez_lending_flashloans table which contains data for all chains.
    Works across all chains - uses GLOBAL_PROJECT_NAME for chain identification.

    Protocols included:
    - Aave V2: ethereum, polygon, avalanche
    - Aave V3: ethereum, polygon, avalanche, arbitrum, optimism, base, gnosis
#}

WITH base_flashloans AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        flashloan_token AS token_address,
        premium_amount,
        COALESCE(premium_amount_usd, 0) AS premium_amount_usd,
        platform,
        modified_timestamp,
        CONCAT(tx_hash, '-', block_number) AS _log_id
    FROM {{ ref('defi__ez_lending_flashloans') }}
    WHERE platform IN ('aave-v2', 'aave-v3')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
)

SELECT
    block_timestamp::date AS date,
    '{{ vars.GLOBAL_PROJECT_NAME }}' AS chain,
    CASE
        WHEN platform = 'aave-v2' THEN 'AAVE V2'
        WHEN platform = 'aave-v3' THEN 'AAVE V3'
    END AS protocol,
    token_address,
    SUM(premium_amount) AS amount_nominal,
    SUM(premium_amount_usd) AS amount_usd,
    MAX(block_number) AS block_number,
    MAX(modified_timestamp) AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base_flashloans
GROUP BY 1, 2, 3, 4
