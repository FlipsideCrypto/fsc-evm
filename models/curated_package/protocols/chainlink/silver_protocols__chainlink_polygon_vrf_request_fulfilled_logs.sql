{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'vrf', 'polygon', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH base AS (
    {{ chainlink_vrf_request_fulfilled_logs(
        'polygon',
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
