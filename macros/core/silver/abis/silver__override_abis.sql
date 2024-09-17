{% macro silver__override_abis() %}
SELECT
    NULL AS contract_address,
    NULL AS DATA
{% endmacro %}
