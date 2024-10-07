{% macro log_bronze_details(source_name, model_type, partition_function, partition_join_key, block_number, uses_receipts_by_hash) %}

{%- if flags.WHICH == 'compile' -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log(source_name ~ model_type ~ '_PARTITION_FUNCTION: ' ~ partition_function, info=True) }}
    {{ log(source_name ~ model_type ~ '_PARTITION_JOIN_KEY: ' ~ partition_join_key, info=True) }}
    {{ log(source_name ~ model_type ~ '_BLOCK_NUMBER: ' ~ block_number, info=True) }}
    {% if uses_receipts_by_hash %}
        {{ log("USES_RECEIPTS_BY_HASH: " ~ uses_receipts_by_hash, info=True) }}
    {% endif %}

    {{ log("", info=True) }}
    {{ log("=== Source Details ===", info=True) }}
    {{ log("Source: " ~ source('bronze_streamline', source_name.lower()), info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{% endmacro %}