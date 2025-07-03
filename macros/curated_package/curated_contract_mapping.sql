{% macro curated_contract_mapping(contract_mapping_dict) %}
    SELECT * FROM VALUES
    {% set all_contracts = [] %}
    {% for protocol, contract_info in contract_mapping_dict.items() %}
        {% if contract_info.contract_address is string %}
            {% set _ = all_contracts.append((contract_info.contract_address, protocol + '-' + contract_info.version, protocol, contract_info.version)) %}
        {% else %}
            {% for contract_address in contract_info.contract_address %}
                {% set _ = all_contracts.append((contract_address, protocol + '-' + contract_info.version, protocol, contract_info.version)) %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {% for contract_tuple in all_contracts %}
        ('{{ contract_tuple[0] }}', '{{ contract_tuple[1] }}', '{{ contract_tuple[2] }}', '{{ contract_tuple[3] }}')
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
    AS t(contract_address, platform, protocol, version)
{% endmacro %}