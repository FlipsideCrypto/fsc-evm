{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'lido', 'token_incentives', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set steth_address = vars.PROTOCOL_LIDO_STETH_ADDRESS %}
{% set ldo_token = vars.PROTOCOL_LIDO_LDO_TOKEN %}

{#
    Lido Token Incentives

    Tracks LDO and stETH token incentives distributed from Lido incentive addresses:
    - 0x87d93d9b2c672bf9c9642d853a8682546a5012b5 (Liquidity Mining)
    - 0x753D5167C31fBEB5b49624314d74A957Eb271709 (Rewards Distribution)
#}

WITH
ldo_prices AS (
    SELECT
        hour
        , price
        , token_address
        , symbol
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE token_address IN (
        LOWER('{{ ldo_token }}'),
        LOWER('{{ steth_address }}')
    )
),
token_transfers AS (
    SELECT
        block_number
        , block_timestamp
        , contract_address
        , raw_amount_precise
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }} t
    WHERE from_address IN (
        LOWER('0x87d93d9b2c672bf9c9642d853a8682546a5012b5'),
        LOWER('0x753D5167C31fBEB5b49624314d74A957Eb271709')
    )
    AND contract_address IN (
        LOWER('{{ ldo_token }}'),
        LOWER('{{ steth_address }}')
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
    DATE(p.hour) AS date
    , 'ethereum' AS chain
    , 'Lido' AS protocol
    , p.symbol AS token
    , p.token_address
    , SUM(COALESCE(t.raw_amount_precise::number / 1e18, 0)) AS amount_native
    , SUM(COALESCE(t.raw_amount_precise::number / 1e18 * p.price, 0)) AS amount_usd
    , MAX(t.block_number) AS block_number
    , MAX(t.modified_timestamp) AS modified_timestamp
FROM ldo_prices p
LEFT JOIN token_transfers t
    ON p.hour = DATE_TRUNC('hour', t.block_timestamp)
    AND p.token_address = t.contract_address
GROUP BY 1, 2, 3, 4, 5
HAVING amount_native > 0
