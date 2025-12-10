{% macro standard_predicate(
        input_column = 'block_number'
    ) -%}
    {%- set tmp_table_name = generate_alias_name(node = model) ~ '__dbt_tmp' -%}
    {%- set database_name = target.database -%}
    {%- set schema_name = generate_schema_name(node = model) -%}
    {%- set full_tmp_table_name = database_name ~ '.' ~ schema_name ~ '.' ~ tmp_table_name -%}
    DBT_INTERNAL_DEST.{{ input_column }} >= (
        SELECT
            MIN(
                {{ input_column }}
            )
        FROM
            {{ full_tmp_table_name }}
    )
{%- endmacro %}
