{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'renzo', 'fees', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    DATE(BLOCK_TIMESTAMP) AS DATE,
    SUM(AMOUNT_USD) AS FEES,
    0.5 * SUM(AMOUNT_USD) AS REVENUE,
    'renzo_protocol' AS APP,
    'DeFi' AS CATEGORY,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_native_transfers') }}
WHERE
    LOWER(FROM_ADDRESS) = LOWER('0xf2F305D14DCD8aaef887E0428B3c9534795D0d60')
    AND LOWER(TO_ADDRESS) = LOWER('0xD22FB2d2c09C108c44b622c37F6d2f4Bc9f85668')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    {% endif %}
GROUP BY DATE(BLOCK_TIMESTAMP)
