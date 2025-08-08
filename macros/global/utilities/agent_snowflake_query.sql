{% macro agent_snowflake_query(sql_query) %}
    {%- if not sql_query.strip().upper().startswith('SELECT') -%}
        {{ exceptions.raise_compiler_error("Only SELECT statements are allowed for agent queries. Received: " ~ sql_query[:50] ~ "...") }}
    {%- endif -%}
    
    {%- set query_result = run_query(sql_query) -%}
    
    {%- if query_result is not none and query_result.rows is not none -%}
        {%- set columns = query_result.column_names -%}
        {%- set rows = query_result.rows -%}
        
        -- Return results as JSON for agent consumption
        {{ return({
            "success": true,
            "columns": columns,
            "rows": rows,
            "row_count": rows | length,
            "query": sql_query
        }) }}
    {%- else -%}
        {{ return({
            "success": false,
            "error": "No results returned",
            "query": sql_query
        }) }}
    {%- endif -%}
{% endmacro %}
