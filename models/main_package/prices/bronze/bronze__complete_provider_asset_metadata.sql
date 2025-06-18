{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','prices','provider','phase_3']
) }}

{# Main query starts here #}
SELECT
    asset_id,
    token_address,
    NAME,
    symbol,
    platform,
    platform_id,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_asset_metadata'
    ) }}
WHERE
    platform IN ({% if vars.MAIN_PRICES_PROVIDER_PLATFORMS is string %}
        '{{ vars.MAIN_PRICES_PROVIDER_PLATFORMS }}'
    {% else %}
        {{ vars.MAIN_PRICES_PROVIDER_PLATFORMS | replace('[', '') | replace(']', '') }}
    {% endif %}) -- platforms specific to the target blockchain
