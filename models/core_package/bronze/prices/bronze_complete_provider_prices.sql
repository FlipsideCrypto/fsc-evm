{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

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
    tags = ['core','bronze','prices']
) }}

{# Main query starts here #}
SELECT
    asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_prices'
    ) }}
    -- prices for all ids, no filter necessary
