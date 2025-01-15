{{ config (
    materialized = "view",
    tags = ['full_test', 'ez_prices_model']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_blocks') }}
