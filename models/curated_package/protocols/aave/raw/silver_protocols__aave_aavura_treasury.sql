{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set aavura_treasury = vars.PROTOCOL_AAVE_AAVURA_TREASURY %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'aavura_treasury', 'curated']
) }}

WITH
tokens AS (
    SELECT LOWER(address) AS address
    FROM (
        VALUES
        ('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'),
        ('0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'),
        ('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
        ('0x5aFE3855358E112B5647B952709E6165e1c1eEEe'),
        ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    ) AS addresses(address)
)
, base AS (
    SELECT
        block_number
        , to_address
        , from_address
        , contract_address
        , block_timestamp::date AS date
        , amount_precise
        , MIN(block_timestamp::date) OVER() AS min_date
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) IN (SELECT address FROM tokens)
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
        , contract_address
        , SUM(CASE WHEN to_address = LOWER('{{ aavura_treasury }}') THEN amount_precise ELSE 0 END) AS amount_in
        , SUM(CASE WHEN from_address = LOWER('{{ aavura_treasury }}') THEN amount_precise ELSE 0 END) AS amount_out
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM base
    GROUP BY 1, 2
)
, prices AS (
    SELECT
        hour::date AS date
        , token_address
        , AVG(price) AS price
    FROM {{ ref('price__ez_prices_hourly') }}
    WHERE token_address IN (SELECT address FROM tokens)
    GROUP BY 1, 2
)

SELECT
    dr.date AS date
    , 'ethereum' AS chain
    , contract_address AS token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (PARTITION BY contract_address ORDER BY dr.date) AS amount_nominal
    , amount_nominal * p.price AS amount_usd
    , f.block_number
    , COALESCE(f.modified_timestamp, SYSDATE()) AS modified_timestamp
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p
    ON p.date = dr.date
    AND LOWER(p.token_address) = LOWER(f.contract_address)
{% if is_incremental() %}
WHERE dr.date >= (
    SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    FROM {{ this }}
)
{% endif %}
