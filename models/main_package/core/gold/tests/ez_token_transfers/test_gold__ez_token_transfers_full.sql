{{ config (
    materialized = "view",
    tags = ['full_test', 'ez_prices_model']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_token_transfers') }}