{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id'],
    cluster_by = ['block_timestamp::date'],
    tags = ['silver_protocols', 'chainlink', 'automation', 'avalanche', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH base AS (
    {{ chainlink_logs(
        'avalanche',
        ('0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6', '0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b'),
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
