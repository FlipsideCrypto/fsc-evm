{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['full_test', 'ez_prices_model']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_native_transfers') }}