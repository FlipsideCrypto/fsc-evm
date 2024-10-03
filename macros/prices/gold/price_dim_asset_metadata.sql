{% macro price_dim_asset_metadata() %}

{# Log configuration details if in dev, during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '"\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view'
) }}

{# Main query starts here #}
SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}
{% endmacro %}
