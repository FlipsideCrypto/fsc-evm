{% macro add_database_or_schema_tags() %}
    {% set prod_db_name = (target.database | replace('_dev', '') | upper) %}
    {{ set_database_tag_value('BLOCKCHAIN_NAME', prod_db_name) }}
    {{ set_database_tag_value('BLOCKCHAIN_TYPE','EVM') }}
{% endmacro %}