{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = get_path_tags(model)
) }}

SELECT
    *
FROM
    {{ ref('core__dim_contract_abis') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())