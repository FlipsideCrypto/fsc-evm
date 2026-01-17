{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'operator_address', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'chainlink', 'rewards', 'arbitrum', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH base AS (
    {{ chainlink_ocr_rewards_daily(
        'arbitrum',
        is_incremental(),
        vars.CURATED_LOOKBACK_HOURS,
        vars.CURATED_LOOKBACK_DAYS
    ) }}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
