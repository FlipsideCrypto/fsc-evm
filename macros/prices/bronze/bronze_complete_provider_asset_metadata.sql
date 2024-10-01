{% macro bronze_complete_provider_asset_metadata() %}

{# Set macro parameters #}
{%- set platforms = var('PRICES_PLATFORMS', platforms) -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view'
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
{% endmacro %}
