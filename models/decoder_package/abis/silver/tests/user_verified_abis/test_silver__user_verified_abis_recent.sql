{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__user_verified_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())