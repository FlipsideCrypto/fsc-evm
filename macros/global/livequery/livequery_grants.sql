{% macro livequery_grants() %}

{% set vars = return_vars() %}
{% set target_db = target.database | upper %}
{% set project = vars.GLOBAL_PROJECT_NAME | upper %}

    {% if var("UPDATE_UDFS_AND_SPS", false) %}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._live TO AWS_LAMBDA_" ~ project ~ "_API;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._live TO AWS_LAMBDA_" ~ project ~ "_API;") %}
        {{ log("Permissions granted to role AWS_LAMBDA_" ~ project ~ "_API for schema " ~ target_db ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._live TO DBT_CLOUD_" ~ project ~ ";") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._live TO DBT_CLOUD_" ~ project ~ ";") %}
        {{ log("Permissions granted to role DBT_CLOUD_" ~ project ~ " for schema " ~ target_db ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._live TO INTERNAL_DEV;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._live TO INTERNAL_DEV;") %}
        {{ log("Permissions granted to role INTERNAL_DEV for schema " ~ target_db ~ "._live", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._utils TO AWS_LAMBDA_" ~ project ~ "_API;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._utils TO AWS_LAMBDA_" ~ project ~ "_API;") %}
        {{ log("Permissions granted to role AWS_LAMBDA_" ~ project ~ "_API for schema " ~ target_db ~ "._utils", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._utils TO DBT_CLOUD_" ~ project ~ ";") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._utils TO DBT_CLOUD_" ~ project ~ ";") %}
        {{ log("Permissions granted to role DBT_CLOUD_" ~ project ~ " for schema " ~ target_db ~ "._utils", info=True) }}

        {% do run_query("GRANT USAGE ON SCHEMA " ~ target_db ~ "._utils TO INTERNAL_DEV;") %}
        {% do run_query("GRANT USAGE ON ALL FUNCTIONS IN SCHEMA " ~ target_db ~ "._utils TO INTERNAL_DEV;") %}
        {{ log("Permissions granted to role INTERNAL_DEV for schema " ~ target_db ~ "._utils", info=True) }}
        
    {% else %}
        {{ log("Error: Permission grants unsuccessful.", info=True) }}
    {% endif %}

{% endmacro %}

{% macro drop_livequery_schemas() %}

{% set target_db = target.database | upper %}

{% if var("UPDATE_UDFS_AND_SPS", false) %}

    {% do run_query("DROP SCHEMA IF EXISTS " ~ target_db ~ "._LIVE;") %}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ target_db ~ "._UTILS;") %}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ target_db ~ ".LIVE;") %}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ target_db ~ ".UTILS;") %}
    {{ log("Schemas dropped successfully.", info=True) }}

{% else %}
    {{ log("Error: DROP SCHEMA unsuccessful.", info=True) }}
{% endif %}

{% endmacro %}