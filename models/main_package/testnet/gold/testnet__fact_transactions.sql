{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['gold_testnet']
) }}

SELECT
    *
FROM
    {{ ref('core__fact_transactions') }}