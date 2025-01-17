{# Set variables #}
{%- set platforms = var('PRICES_PROVIDER_PLATFORMS', '') -%}

{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    
    {{ log("PRICES_PROVIDER_PLATFORMS: " ~ platforms, info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_prices']
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
