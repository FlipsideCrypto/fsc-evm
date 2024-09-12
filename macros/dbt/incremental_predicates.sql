{% macro standard_predicate(
        input_column = 'block_number'
    ) -%}
    {%- set tmp_table_name = this.identifier ~ '__dbt_tmp' -%}
    {{ this }}.{{ input_column }} >= (
        SELECT
            MIN(
                {{ input_column }}
            )
        FROM
            {{ this.database }}.{{ this.schema }}.{{ tmp_table_name }}
    )
{%- endmacro %}
