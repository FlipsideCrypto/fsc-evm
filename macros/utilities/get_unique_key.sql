{% macro get_unique_key() %}
    {%- set uses_receipts_by_hash = var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}
    {%- set unique_key = 'tx_hash' if uses_receipts_by_hash else 'block_number' -%}
    {{ return(unique_key) }}
{% endmacro %} 