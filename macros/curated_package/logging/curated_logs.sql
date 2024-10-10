{% macro log_prices_bronze_details(platforms, symbols, token_addresses, blockchains) %}

{%- if flags.WHICH == 'compile' and execute -%}

    {% if platforms or symbols or token_addresses or blockchains %}
    {{ log("=== Current Variable Settings ===", info=True) }}

    {% if platforms %}
        {{ log("PRICES_PLATFORMS: " ~ platforms, info=True) }}
    {% endif %}
    {% if symbols %}
        {{ log("PRICES_SYMBOLS: " ~ symbols, info=True) }}
    {% endif %}
    {% if token_addresses %}
        {{ log("PRICES_TOKEN_ADDRESSES: " ~ token_addresses, info=True) }}
    {% endif %}
    {% if blockchains %}
        {{ log("PRICES_BLOCKCHAINS: " ~ blockchains, info=True) }}
    {% endif %}
        {{ log("", info=True) }}
    {% endif %}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% if config.get('incremental_strategy') or config.get('unique_key') or config.get('cluster_by') or config.get('post_hook') or config.get('tags') %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% if config.get('incremental_strategy') %}
        {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '",\n' %}
    {% endif %}
    {% if config.get('unique_key') %}
        {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}    
    {% endif %}
    {% if config.get('cluster_by') %}
        {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
    {% endif %}
    {% if config.get('post_hook') %}
        {% set config_log = config_log ~ '    post_hook = "' ~ config.get('post_hook') ~ '",\n' %}
    {% endif %}
    {% if config.get('meta') %}
        {% set config_log = config_log ~ '    meta = ' ~ config.get('meta') ~ ',\n' %}
    {% endif %}
    {% if config.get('tags') %}
        {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% endif %}
    {% else%}
        {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '"\n' %}
    {% endif %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{% endmacro %}

    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }