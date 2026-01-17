{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token', 'tx_hash'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'goldfinch', 'token_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Token Incentives

    Tracks GFI token incentives distributed from Goldfinch incentive addresses:
    - 0x384860F14B39CcD9C89A73519c70cD5f5394D0a6
    - 0x0Cd73c18C085dEB287257ED2307eC713e9Af3460
    - 0xFD6FF39DA508d281C2d255e9bBBfAb34B6be60c3
    GFI Token: 0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b
#}

SELECT
    DATE(block_timestamp) AS date,
    symbol AS token,
    amount AS amount_native,
    amount_usd,
    tx_hash,
    modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_token_transfers') }}
WHERE from_address IN (
    LOWER('0x384860F14B39CcD9C89A73519c70cD5f5394D0a6'),
    LOWER('0x0Cd73c18C085dEB287257ED2307eC713e9Af3460'),
    LOWER('0xFD6FF39DA508d281C2d255e9bBBfAb34B6be60c3')
)
AND contract_address = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b')
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
    FROM {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
{% endif %}
