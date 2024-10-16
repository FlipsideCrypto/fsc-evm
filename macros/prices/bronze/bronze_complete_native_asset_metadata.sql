{% macro bronze_complete_native_asset_metadata() %}

{# Set macro parameters #}
{%- set symbols = var('PRICES_SYMBOLS', symbols) -%}
{%- set blockchains = var('PRICES_BLOCKCHAINS', var('PROD_DB_NAME') ) -%}

{# Log configuration details if in dev, during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log("PRICES_SYMBOLS: " ~ symbols, info=True) }}
    {{ log("PRICES_BLOCKCHAINS: " ~ blockchains, info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '"\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view'
) }}

{# Main query starts here #}
SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_asset_metadata'
    ) }}
WHERE
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND symbol IN ({% if symbols is string %}
        '{{ symbols }}'
    {% else %}
        {{ symbols | replace('[', '') | replace(']', '') }}
    {% endif %})
{% endmacro %}
