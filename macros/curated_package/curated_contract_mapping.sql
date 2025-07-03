{% macro curated_contract_mapping(contract_mapping_dict) %}
    SELECT * FROM VALUES
    {% set all_contracts = [] %}
    {% for protocol, versions in contract_mapping_dict.items() %}
        {% for version, version_info in versions.items() %}
            {% if version_info.contract_address is string %}
                {% set _ = all_contracts.append((version_info.contract_address, protocol, version)) %}
            {% else %}
                {% for contract_address in version_info.contract_address %}
                    {% set _ = all_contracts.append((contract_address, protocol, version)) %}
                {% endfor %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% for contract_tuple in all_contracts %}
        ('{{ contract_tuple[0] }}', '{{ contract_tuple[1] }}', '{{ contract_tuple[2] }}')
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
    AS t(contract_address, protocol, version)
{% endmacro %}