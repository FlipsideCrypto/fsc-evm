{% macro curated_contract_mapping(contract_mapping_dict) %}
    SELECT * FROM VALUES
    {% set all_contracts = [] %}
    {% for protocol, versions in contract_mapping_dict.items() %}
        {% for version, version_info in versions.items() %}
            {% for type, type_info in version_info.items() %}
                {% if type_info is string %}
                    {% set _ = all_contracts.append((type_info, protocol, version, type)) %}
                {% else %}
                    {% for contract_address in type_info %}
                        {% set _ = all_contracts.append((contract_address, protocol, version, type)) %}
                    {% endfor %}
                {% endif %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
    {% for contract_tuple in all_contracts %}
        ('{{ contract_tuple[0] }}', '{{ contract_tuple[1] }}', '{{ contract_tuple[2] }}', '{{ contract_tuple[3] }}')
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
    AS t(contract_address, protocol, version, type)
{% endmacro %}