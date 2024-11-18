{% macro release_chain(schema_name, role_name) %}

{% set prod_db_name = var('GLOBAL_PROD_DB_NAME', '') | upper %}

    {% if target.database | upper == prod_db_name and target.name == 'prod' %}
        {% do run_query("GRANT USAGE ON DATABASE " ~ prod_db_name ~ " TO ROLE " ~ role_name ~ " COPY CURRENT GRANTS;") %}
        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ " COPY CURRENT GRANTS;") %}
        {% do run_query("GRANT SELECT ON ALL TABLES IN SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ " COPY CURRENT GRANTS;") %}
        {% do run_query("GRANT SELECT ON ALL VIEWS IN SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ " COPY CURRENT GRANTS;") %}
        {% do run_query("GRANT SELECT ON ALL FUTURE TABLES IN SCHEMA " ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT SELECT ON ALL FUTURE VIEWS IN SCHEMA " ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% log "Granted SELECT on all future tables and views in schema " ~ schema_name ~ " to role " ~ role_name %}
    {% else %}
        {% log "Not granting SELECT on all future tables and views in schema " ~ schema_name ~ " to role " ~ role_name ~ " because target is not prod" %}
    {% endif %}

{% endmacro %}