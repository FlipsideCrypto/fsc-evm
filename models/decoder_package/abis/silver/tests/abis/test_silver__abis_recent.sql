{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__abis') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())