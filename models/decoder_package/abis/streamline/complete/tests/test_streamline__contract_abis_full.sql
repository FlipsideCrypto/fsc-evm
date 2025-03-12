{# Log configuration details #}
{{ log_model_details() }}
{{ config (
    materialized = "view",
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('streamline__complete_contract_abis') }}
