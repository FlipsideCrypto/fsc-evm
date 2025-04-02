{% macro create_evm_streamline_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% do run_query("CREATE SCHEMA IF NOT EXISTS streamline") %}
        
        {{ create_udf_bulk_rest_api_v2() }}
        {{ create_udf_bulk_decode_logs() }}
        {{ create_udf_bulk_decode_traces() }}
    {% endif %}
{% endmacro %}