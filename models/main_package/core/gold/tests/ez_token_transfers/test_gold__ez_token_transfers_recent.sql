{{ config (
    materialized = "view",
    tags = ['recent_test', 'ez_prices_model']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_token_transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
