{% macro add_database_or_schema_tags() %}
    {% set prod_db_name = var('GLOBAL_PROD_DB_NAME', '') | upper %}
    {{ set_database_tag_value('BLOCKCHAIN_NAME', prod_db_name) }}
    {{ set_database_tag_value('BLOCKCHAIN_TYPE','EVM') }}
{% endmacro %}