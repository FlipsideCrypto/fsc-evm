{% macro curated_contract_mapping(mapping_vars) %}
    SELECT * FROM VALUES
    {% for var_name, var_value in mapping_vars.items() %}
        {% if var_value %}
            ('{{ var_value[0] }}', '{{ var_value[1] }}', '{{ var_value[2] }}', '{{ var_value[3] }}')
            {%- if not loop.last -%},{%- endif %}
        {% endif %}
    {% endfor %}
    AS t(contract_address, platform, protocol, version)
{% endmacro %}