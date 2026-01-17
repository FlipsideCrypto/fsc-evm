{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'gho_treasury_revenue', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set gho_treasury = vars.PROTOCOL_AAVE_GHO_TREASURY %}

WITH
event_logs AS (
    SELECT
        block_number
        , block_timestamp
        , '0x' || SUBSTR(topics[2]::string, 27, 40) AS asset
        , utils.udf_hex_to_int(data) AS amount
        , modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE contract_address = LOWER('{{ gho_treasury }}')
        AND topics[0]::string = '0xb29fcda740927812f5a71077b62e132bead3769a455319c29b9a1cc461a65475'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
)
, priced_logs AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , asset
        , amount / POW(10, decimals) AS amount_nominal
        , amount_nominal * price AS amount_usd
        , e.modified_timestamp
    FROM event_logs e
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON date_trunc(hour, block_timestamp) = hour
        AND LOWER(asset) = LOWER(token_address)
)
SELECT
    date
    , 'AAVE GHO' AS protocol
    , 'ethereum' AS chain
    , asset AS token_address
    , SUM(COALESCE(amount_nominal, 0)) AS amount_nominal
    , SUM(COALESCE(amount_usd, 0)) AS amount_usd
    , MAX(block_number) AS block_number
    , MAX(modified_timestamp) AS modified_timestamp
FROM priced_logs
GROUP BY 1, 2, 3, 4
