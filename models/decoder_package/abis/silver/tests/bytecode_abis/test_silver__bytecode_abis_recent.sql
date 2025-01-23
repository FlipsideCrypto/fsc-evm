{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__bytecode_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
