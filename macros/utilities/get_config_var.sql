{% macro get_config_var(var_name) %}
  {% if var_name == 'USES_RECEIPTS_BY_HASH' %}
    {% set value = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
    {{ return(value) }}
  {% elif var_name == 'MAIN_CORE_RECEIPTS_SOURCE_NAME' %}
    {% set uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
    {% set value = 'RECEIPTS_BY_HASH' if uses_receipts_by_hash else 'RECEIPTS' %}
    {{ return(value) }}
  {% elif var_name == 'MAIN_CORE_UNIQUE_KEY' %}
    {% set uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) %}
    {% set value = 'tx_hash' if uses_receipts_by_hash else 'block_number' %}
    {{ return(value) }}
  {% else %}
    {{ return(none) }}
  {% endif %}
{% endmacro %}