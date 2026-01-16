{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Protocol-specific addresses #}
{% set incentives_controller = vars.PROTOCOL_AAVE_INCENTIVES_CONTROLLER_ETHEREUM %}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'aave', 'ecosystem_incentives', 'curated']
) }}

{{ aave_v2_ecosystem_incentives(
    'ethereum',
    incentives_controller,
    'AAVE V2',
    is_incremental(),
    vars.CURATED_LOOKBACK_HOURS,
    vars.CURATED_LOOKBACK_DAYS
) }}
