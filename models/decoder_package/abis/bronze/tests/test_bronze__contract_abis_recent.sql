{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('bronze_api__contract_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())