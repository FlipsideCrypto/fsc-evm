{% macro run_sp_create_prod_clone() %}
    {% set prod_db_name = var('GLOBAL_PROD_DB_NAME', '') | upper %}
    {% set dev_suffix = var('DEV_DATABASE_SUFFIX', '_DEV') %}
    {% set clone_role = var('CLONE_ROLE', 'internal_dev') %}

    {% set clone_query %}
    call {{ prod_db_name }}._internal.create_prod_clone(
        '{{ prod_db_name }}',
        '{{ prod_db_name }}{{ dev_suffix }}',
        '{{ clone_role }}'
    );
    {% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}