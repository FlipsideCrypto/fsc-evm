{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','core','full_test','transfers','ez']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_token_transfers') }}