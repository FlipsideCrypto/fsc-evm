{% macro curated_contract_mapping(contract_mapping_dict) %}
    SELECT * FROM VALUES
    {% for protocol, contract_info in contract_mapping_dict.items() %}
        {% for contract_address in contract_info.contract_address %}
            ('{{ contract_address }}', '{{ protocol }}-{{ contract_info.version }}', '{{ protocol }}', '{{ contract_info.version }}')
            {%- if not (loop.last and loop.outer.last) -%},{%- endif %}
        {% endfor %}
    {% endfor %}
    AS t(contract_address, platform, protocol, version)
{% endmacro %}