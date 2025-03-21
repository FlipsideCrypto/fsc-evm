{% macro release_chain(schema_name, role_name) %}

{% set prod_db_name = (target.database | replace('_dev', '') | upper) %}

    {% if target.database | upper == prod_db_name and target.name == 'prod' %}
        {% do run_query("GRANT USAGE ON DATABASE " ~ prod_db_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT SELECT ON ALL TABLES IN SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT SELECT ON ALL VIEWS IN SCHEMA " ~ prod_db_name ~ "." ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT SELECT ON FUTURE TABLES IN SCHEMA " ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {% do run_query("GRANT SELECT ON FUTURE VIEWS IN SCHEMA " ~ schema_name ~ " TO ROLE " ~ role_name ~ ";") %}
        {{ log("Permissions granted to role " ~ role_name ~ " for schema " ~ schema_name, info=True) }}
    {% else %}
        {{ log("Not granting SELECT on future tables and views in schema " ~ schema_name ~ " to role " ~ role_name ~ " because target is not prod", info=True) }}
    {% endif %}

{% endmacro %}