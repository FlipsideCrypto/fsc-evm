{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contract_abis') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())