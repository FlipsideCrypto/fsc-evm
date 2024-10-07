{% macro create_sps() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) %}
        {% set prod_db_name = var('PROD_DB_NAME') | upper %}
        {% if target.database | upper == prod_db_name and target.name == 'prod' %}
            {% set schema_name = var("SPS_SCHEMA_NAME", '_internal') %}
            {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ schema_name) %}
            {{ sp_create_prod_clone(schema_name) }}
        {% endif %}
    {% endif %}
{% endmacro %}