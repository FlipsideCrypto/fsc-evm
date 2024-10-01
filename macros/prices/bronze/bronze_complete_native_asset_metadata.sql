{% macro bronze_complete_native_asset_metadata() %}

{# Set macro parameters #}
{%- set symbols = var('PRICES_SYMBOLS', symbols) -%}
{%- set blockchains = var('PRICES_BLOCKCHAINS', target.database | lower | replace('_dev', '') ) -%}

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
