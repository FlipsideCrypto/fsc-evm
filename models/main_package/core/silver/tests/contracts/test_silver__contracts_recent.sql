{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__contracts') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
