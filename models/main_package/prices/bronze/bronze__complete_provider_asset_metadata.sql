{# Set variables #}
{%- set platforms = var('PRICES_PROVIDER_PLATFORMS', '') -%}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_prices','phase_2']
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
    platform IN ({% if platforms is string %}
        '{{ platforms }}'
    {% else %}
        {{ platforms | replace('[', '') | replace(']', '') }}
    {% endif %}) -- platforms specific to the target blockchain
