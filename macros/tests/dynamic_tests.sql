{% macro setup_test_filter() %}
  {% set create_view %}
    create or replace view test.test_timestamp_filters as (
      with test_data as (
        select 
          table_schema,
          table_name,
          listagg(column_name, ', ') within group (
            order by 
              case column_name
                when 'MODIFIED_TIMESTAMP' then 1
                when '_INSERTED_TIMESTAMP' then 2 
                when 'BLOCK_TIMESTAMP' then 3
              end
          ) as timestamp_columns
        from information_schema.columns
        where column_name in ('MODIFIED_TIMESTAMP', '_INSERTED_TIMESTAMP', 'BLOCK_TIMESTAMP')
        group by table_schema, table_name
      )
      select 
        table_schema,
        table_name,
        timestamp_columns,
        split_part(timestamp_columns, ', ', 1) as primary_timestamp_column,
        case 
          when split_part(timestamp_columns, ', ', 1) = 'MODIFIED_TIMESTAMP' 
            then 'MODIFIED_TIMESTAMP >= dateadd({{ var("interval_type", "day") }}, -{{ var("interval_value", 7) }}, current_timestamp())'
          when split_part(timestamp_columns, ', ', 1) = '_INSERTED_TIMESTAMP' 
            then '_INSERTED_TIMESTAMP >= dateadd({{ var("interval_type", "day") }}, -{{ var("interval_value", 7) }}, current_timestamp())'
          when split_part(timestamp_columns, ', ', 1) = 'BLOCK_TIMESTAMP' 
            then 'BLOCK_TIMESTAMP >= dateadd({{ var("interval_type", "day") }}, -{{ var("interval_value", 7) }}, current_timestamp())'
          else 'true'
        end as filter_condition
      from test_data
    )
  {% endset %}

  {% do run_query(create_view) %}
  {{ log("Created view test.test_timestamp_filters for test filtering with interval_type=" ~ var("interval_type", "day") ~ " and interval_value=" ~ var("interval_value", 7), info=True) }}
{% endmacro %}

{% macro get_where_subquery(relation) -%}
    {% set where = config.get('where') %}
    {% set dynamic = var('dynamic', false) %}
    
    {# Only apply dynamic filtering if dynamic variable is true #}
    {% if not dynamic %}
        {% if where %}
            {%- set filtered -%}
                (select * from {{ relation }} where {{ where }}) dbt_subquery
            {%- endset -%}
            {% do return(filtered) %}
        {%- else -%}
            {% do return(relation) %}
        {%- endif -%}
    {% endif %}
    
    {# Check if this is a data type test #}
    {% if 'dbt_expectations_expect_column_values_to_be_in_type_list' in this | string %}
        {% do return(relation) %}
    {% endif %}
    
    {% if where %}
        {% if "__timestamp_filter__" in where %}
            {% set filter_query %}
                select filter_condition 
                from test.test_timestamp_filters 
                where table_schema = UPPER('{{ relation.schema }}')
                and table_name = UPPER('{{ relation.identifier }}')
            {% endset %}
            
            {% set results = run_query(filter_query) %}
            
            {% if results.columns[0].values() | length > 0 %}
                {% set filter_condition = results.columns[0].values()[0] %}
                {% set where = where | replace("__timestamp_filter__", filter_condition) %}
            {% endif %}
        {% endif %}
        
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}