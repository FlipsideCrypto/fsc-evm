{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = get_path_tags(model)
) }}

SELECT
    *
FROM
    {{ ref('bronze_api__contract_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())