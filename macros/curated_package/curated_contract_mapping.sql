{% macro curated_contract_mapping(contract_mapping_dict) %}
    SELECT * FROM VALUES
    {% for key, contract_info in contract_mapping_dict.items() %}
        ('{{ contract_info.contract_address }}', '{{ contract_info.platform }}', '{{ contract_info.protocol }}', '{{ contract_info.version }}')
        {%- if not loop.last -%},{%- endif %}
    {% endfor %}
    AS t(contract_address, platform, protocol, version)
{% endmacro %}