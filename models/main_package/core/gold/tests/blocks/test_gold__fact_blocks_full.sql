{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['gold','test_gold','core','full_test']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_blocks') }}
