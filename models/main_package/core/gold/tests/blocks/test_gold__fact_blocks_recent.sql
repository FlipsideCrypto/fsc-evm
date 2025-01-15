{{ config (
    materialized = "view",
    tags = ['recent_test', 'ez_prices_model']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_blocks') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
