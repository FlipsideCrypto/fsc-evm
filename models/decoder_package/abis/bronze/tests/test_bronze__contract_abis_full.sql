{{ config (
    materialized = "view",
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('bronze_api__contract_abis') }}