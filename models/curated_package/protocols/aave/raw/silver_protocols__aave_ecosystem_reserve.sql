{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set aave_token = vars.PROTOCOL_AAVE_TOKEN_ADDRESS %}
{% set ecosystem_reserve = vars.PROTOCOL_AAVE_ECOSYSTEM_RESERVE %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'ecosystem_reserve', 'curated']
) }}

WITH
base AS (
    SELECT
        block_number
        , to_address
        , from_address
        , block_timestamp::date AS date
        , amount_precise
        , MIN(block_timestamp::date) OVER() AS min_date
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('{{ aave_token }}')
)
, date_range AS (
    SELECT *
    FROM (
        SELECT
            min_date + SEQ4() AS date
        FROM base
    )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows AS (
    SELECT
        date
        , SUM(CASE WHEN to_address = LOWER('{{ ecosystem_reserve }}') THEN amount_precise ELSE 0 END) AS amount_in
        , SUM(CASE WHEN from_address = LOWER('{{ ecosystem_reserve }}') THEN amount_precise ELSE 0 END) AS amount_out
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM base
    GROUP BY 1
)
, prices AS ({{ get_coingecko_price_with_latest('aave') }})

SELECT
    dr.date AS date
    , 'ethereum' AS chain
    , '{{ aave_token }}' AS token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) AS amount_nominal
    , amount_nominal * p.price AS amount_usd
    , f.block_number
    , COALESCE(f.modified_timestamp, SYSDATE()) AS modified_timestamp
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p ON p.date = dr.date
{% if is_incremental() %}
WHERE dr.date >= (
    SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    FROM {{ this }}
)
{% endif %}
