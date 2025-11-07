{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view',
    tags = ['gold','chain_stats','curated','phase_4']
) }}

SELECT
    *
FROM
    crosschain.chain_stats.ez_{{ vars.GLOBAL_PROJECT_NAME }}_protocol_metrics