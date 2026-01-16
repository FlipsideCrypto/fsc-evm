{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set stkaave = vars.PROTOCOL_AAVE_SAFETY_MODULE_STKAAVE %}
{% set stkabpt = vars.PROTOCOL_AAVE_SAFETY_MODULE_STKABPT %}
{% set stkgho = vars.PROTOCOL_AAVE_SAFETY_MODULE_STKGHO %}

{% set start_date = '2018-01-01' %}
{% set end_date = modules.datetime.date.today().isoformat() %}
{% set days_between = (modules.datetime.datetime.strptime(end_date, '%Y-%m-%d') - modules.datetime.datetime.strptime(start_date, '%Y-%m-%d')).days %}
{% set rowcount = days_between + 1 %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'safety_module', 'curated']
) }}

WITH
stkAAVE AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , LOWER('{{ stkaave }}') AS token
        , CASE
            WHEN to_address = '0x0000000000000000000000000000000000000000' THEN -amount
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount
        END AS mint
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('{{ stkaave }}')
        AND (
            to_address = '0x0000000000000000000000000000000000000000'
            OR from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkABPT_mints AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , LOWER('{{ stkabpt }}') AS token
        , CASE
            WHEN to_address = '0x0000000000000000000000000000000000000000' THEN -amount
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount
        END AS mint
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('{{ stkabpt }}')
        AND (
            to_address = '0x0000000000000000000000000000000000000000'
            OR from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkGHO_mints AS (
    SELECT
        block_number
        , block_timestamp::date AS date
        , LOWER('{{ stkgho }}') AS token
        , CASE
            WHEN to_address = '0x0000000000000000000000000000000000000000' THEN -amount
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount
        END AS mint
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE LOWER(contract_address) = LOWER('{{ stkgho }}')
        AND (
            to_address = '0x0000000000000000000000000000000000000000'
            OR from_address = '0x0000000000000000000000000000000000000000'
        )
)
, tokens AS (
    SELECT LOWER('{{ stkaave }}') AS token
    UNION
    SELECT LOWER('{{ stkabpt }}') AS token
    UNION
    SELECT LOWER('{{ stkgho }}') AS token
)
, dt_spine AS (
    SELECT '2018-01-01'::date + seq4() AS date
    FROM TABLE(GENERATOR(ROWCOUNT => {{ rowcount }}))
    WHERE date <= TO_DATE(SYSDATE())
)
, token_days AS (
    SELECT tokens.token, dt_spine.date
    FROM tokens
    CROSS JOIN dt_spine
)
, all_mints AS (
    SELECT * FROM stkAAVE
    UNION ALL
    SELECT * FROM stkABPT_mints
    UNION ALL
    SELECT * FROM stkGHO_mints
)
, daily_mint AS (
    SELECT
        date
        , token
        , SUM(mint) AS daily_mint
        , MAX(block_number) AS block_number
        , MAX(modified_timestamp) AS modified_timestamp
    FROM all_mints
    GROUP BY date, token
)
, daily_mints_filled AS (
    SELECT
        token_days.token
        , token_days.date
        , COALESCE(daily_mint.daily_mint, 0) AS daily_mint
        , daily_mint.block_number
        , daily_mint.modified_timestamp
    FROM token_days
    LEFT JOIN daily_mint
        ON daily_mint.date = token_days.date
        AND LOWER(daily_mint.token) = LOWER(token_days.token)
)
, result AS (
    SELECT
        token
        , date
        , daily_mint
        , SUM(daily_mint) OVER(PARTITION BY token ORDER BY date) AS total_supply
        , block_number
        , modified_timestamp
    FROM daily_mints_filled
)
, aave_prices AS ({{ get_coingecko_price_with_latest("aave") }})
, gho_prices AS ({{ get_coingecko_price_with_latest("gho") }})
, abpt_prices AS ({{ get_coingecko_price_with_latest("aave-balancer-pool-token") }})
, prices AS (
    SELECT date, '0x4da27a545c0c5b758a6ba100e3a049001de870f5' AS token, price
    FROM aave_prices

    UNION ALL

    SELECT date, '0xa1116930326d21fb917d5a27f1e9943a9595fb47' AS token, price
    FROM abpt_prices

    UNION ALL

    SELECT date, '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d' AS token, price
    FROM gho_prices
)

SELECT
    result.date
    , 'ethereum' AS chain
    , result.token AS token_address
    , total_supply AS amount_nominal
    , COALESCE(prices.price, 0) * total_supply AS amount_usd
    , result.block_number
    , COALESCE(result.modified_timestamp, SYSDATE()) AS modified_timestamp
FROM result
LEFT JOIN prices
    ON prices.token = result.token
    AND prices.date = result.date
{% if is_incremental() %}
WHERE result.date >= (
    SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    FROM {{ this }}
)
{% endif %}
