{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','abis','full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__bytecode_abis') }}
