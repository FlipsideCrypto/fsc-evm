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
{% if vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
    {{ source('crosschain_chain_stats', 'ez_arbitrum_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'avalanche' %}
    {{ source('crosschain_chain_stats', 'ez_avalanche_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'base' %}
    {{ source('crosschain_chain_stats', 'ez_base_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'bob' %}
    {{ source('crosschain_chain_stats', 'ez_bob_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'boba' %}
    {{ source('crosschain_chain_stats', 'ez_boba_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'bsc' %}
    {{ source('crosschain_chain_stats', 'ez_bsc_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    {{ source('crosschain_chain_stats', 'ez_ethereum_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'ink' %}
    {{ source('crosschain_chain_stats', 'ez_ink_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'optimism' %}
    {{ source('crosschain_chain_stats', 'ez_optimism_address_metrics') }}
{% elif vars.GLOBAL_PROJECT_NAME == 'polygon' %}
    {{ source('crosschain_chain_stats', 'ez_polygon_address_metrics') }}
{% endif %}