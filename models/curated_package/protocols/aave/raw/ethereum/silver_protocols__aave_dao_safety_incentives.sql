{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set stkaave = vars.PROTOCOL_AAVE_SAFETY_MODULE_STKAAVE %}
{% set aave_token = vars.PROTOCOL_AAVE_TOKEN_ADDRESS %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'dao_safety_incentives', 'curated']
) }}

WITH
logs AS (
    SELECT
        block_number
        , block_timestamp
        , decoded_log:amount::float / 1E18 AS amount_nominal
        , modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('{{ stkaave }}')
        AND event_name = 'RewardsClaimed'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
, prices AS ({{ get_coingecko_price_with_latest('aave') }})
, priced_logs AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , '{{ aave_token }}' AS token_address
        , amount_nominal
        , amount_nominal * price AS amount_usd
        , l.modified_timestamp
    FROM logs l
    LEFT JOIN prices ON block_timestamp::date = prices.date
)
SELECT
    date
    , token_address
    , 'AAVE DAO' AS protocol
    , 'ethereum' AS chain
    , SUM(COALESCE(amount_nominal, 0)) AS amount_nominal
    , SUM(COALESCE(amount_usd, 0)) AS amount_usd
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
FROM priced_logs
GROUP BY 1, 2
