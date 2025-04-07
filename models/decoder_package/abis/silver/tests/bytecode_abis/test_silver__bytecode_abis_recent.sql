{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','abis','daily_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('silver__bytecode_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
