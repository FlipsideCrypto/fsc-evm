{% macro price_fact_prices_ohlc_hourly() %}

{# Log configuration details if in dev, during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    persist_docs = ' ~ config.get('persist_docs') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

{# Main query starts here #}
SELECT
    asset_id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id AS fact_prices_ohlc_hourly_id
FROM
    {{ ref('silver__complete_provider_prices') }}
{% endmacro %}
