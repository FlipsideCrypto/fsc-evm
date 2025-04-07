{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['silver','test_silver','core','daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__proxies') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())