{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'liquity', 'token_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Liquity Token Incentives

    Tracks LQTY token incentives distributed from Liquity incentive addresses:
    - 0xd37a77E71ddF3373a79BE2eBB76B6c4808bDF0d5 (Stability Pool)
    - 0xD8c9D9071123a059C6E0A945cF0e0c82b508d816 (Frontend Operators)
#}

WITH token_transfers AS (
    SELECT
        block_number
        , block_timestamp
        , contract_address
        , symbol
        , amount
        , amount_usd
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0x6dea81c8171d0ba574754ef6f8b412f2ed88c54d')
    AND from_address IN (
        LOWER('0xd37a77E71ddF3373a79BE2eBB76B6c4808bDF0d5'),
        LOWER('0xD8c9D9071123a059C6E0A945cF0e0c82b508d816')
    )
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)

SELECT
    block_timestamp::DATE AS date
    , 'ethereum' AS chain
    , 'Liquity' AS protocol
    , symbol AS token
    , SUM(amount) AS token_incentives_native
    , SUM(amount_usd) AS token_incentives
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
    , SYSDATE() AS inserted_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM token_transfers
GROUP BY 1, 2, 3, 4
HAVING token_incentives_native > 0
