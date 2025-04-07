{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','test_silver','abis','daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__user_verified_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())