{% macro create_sps() %}
    {{ log("Debug: Entering create_sps macro", info=True) }}
    {{ log("Debug: Available macros: " ~ context.keys() | join(", "), info=True) }}
    {% if var("UPDATE_UDFS_AND_SPS", false) %}
        {{ log("Debug: UPDATE_UDFS_AND_SPS is true", info=True) }}
        {% set prod_db_name = var('PROD_DB_NAME') | upper %}
        {{ log("Debug: prod_db_name is " ~ prod_db_name, info=True) }}
        {% if target.database | upper == prod_db_name and target.name == 'prod' %}
            {{ log("Debug: Target database and name match production criteria", info=True) }}
            {% set schema_name = var("SPS_SCHEMA_NAME", '_internal') %}
            {{ log("Debug: schema_name is " ~ schema_name, info=True) }}
            {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ schema_name) %}
            {{ log("Debug: Attempting to call sp_create_prod_clone", info=True) }}
            {% if execute %}
                {% if context.get('fsc_evm.sp_create_prod_clone') is not none %}
                    {{ log("Debug: sp_create_prod_clone is defined", info=True) }}
                    {{ fsc_evm.sp_create_prod_clone(schema_name) }}
                {% else %}
                    {{ log("Warning: fsc_evm.sp_create_prod_clone is not defined. Skipping stored procedure creation.", info=True) }}
                {% endif %}
            {% else %}
                {{ log("Debug: In compilation phase, skipping sp_create_prod_clone call", info=True) }}
            {% endif %}
        {% else %}
            {{ log("Debug: Target database or name do not match production criteria", info=True) }}
        {% endif %}
    {% else %}
        {{ log("Debug: UPDATE_UDFS_AND_SPS is false", info=True) }}
    {% endif %}
    {{ log("Debug: Exiting create_sps macro", info=True) }}
{% endmacro %}