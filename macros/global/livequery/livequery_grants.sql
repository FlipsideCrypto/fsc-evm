{% macro livequery_grants() %}

{% set prod_db_name = (target.database | replace('_dev', '') | upper) %}

    {% if var("UPDATE_UDFS_AND_SPS", false) and target.database | upper == prod_db_name and target.name == 'prod' %}
    
        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._live TO AWS_LAMBDA_" ~ prod_db_name ~ "_API;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._live TO AWS_LAMBDA_" ~ prod_db_name ~ "_API;") %}
        {{ log("Permissions granted to role AWS_LAMBDA_" ~ prod_db_name ~ "_API for schema " ~ prod_db_name ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._live TO DBT_CLOUD_" ~ prod_db_name ~ ";") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._live TO DBT_CLOUD_" ~ prod_db_name ~ ";") %}
        {{ log("Permissions granted to role DBT_CLOUD_" ~ prod_db_name ~ " for schema " ~ prod_db_name ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._live TO INTERNAL_DEV;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._live TO INTERNAL_DEV;") %}
        {{ log("Permissions granted to role INTERNAL_DEV for schema " ~ prod_db_name ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._utils TO AWS_LAMBDA_" ~ prod_db_name ~ "_API;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._utils TO AWS_LAMBDA_" ~ prod_db_name ~ "_API;") %}
        {{ log("Permissions granted to role AWS_LAMBDA_" ~ prod_db_name ~ "_API for schema " ~ prod_db_name ~ "._utils", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._utils TO DBT_CLOUD_" ~ prod_db_name ~ ";") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._utils TO DBT_CLOUD_" ~ prod_db_name ~ ";") %}
        {{ log("Permissions granted to role DBT_CLOUD_" ~ prod_db_name ~ " for schema " ~ prod_db_name ~ "._utils", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ prod_db_name ~ "._utils TO INTERNAL_DEV;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ prod_db_name ~ "._utils TO INTERNAL_DEV;") %}
        {{ log("Permissions granted to role INTERNAL_DEV for schema " ~ prod_db_name ~ "._utils", info=True) }}
        
    {% else %}
        {{ log("Error: Permission grants unsuccessful. Check if target is prod.", info=True) }}
    {% endif %}

{% endmacro %}

{% macro drop_livequery_schemas() %}

{% if var("UPDATE_UDFS_AND_SPS", false) and target.database | upper == prod_db_name and target.name == 'prod' %}

    {% set drop_schemas_sql %}
        DROP SCHEMA IF EXISTS _LIVE;
        DROP SCHEMA IF EXISTS _UTILS;
        DROP SCHEMA IF EXISTS LIVE;
        DROP SCHEMA IF EXISTS UTILS;
    {% endset %}
    {% do run_query(drop_schemas_sql) %}

{% else %}
    {{ log("Error: DROP SCHEMA unsuccessful. Check if target is prod.", info=True) }}
{% endif %}

{% endmacro %}