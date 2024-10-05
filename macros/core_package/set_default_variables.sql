{% macro set_default_variables(model_name, model_type) %}

{%- set node_url = var('NODE_URL', '{Service}/{Authentication}') -%}
{%- set model_quantum_state = var((model_name ~ '_' ~ model_type ~ '_quantum_state').upper(), 'streamline') -%}
{%- set testing_limit = var((model_name ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}
{%- set new_build = var((model_name ~ '_' ~ model_type ~ '_new_build').upper(), false) -%}
{%- set default_order = 'ORDER BY partition_key DESC, block_number DESC' if model_type.lower() == 'realtime' 
    else 'ORDER BY partition_key ASC, block_number ASC' -%}
{%- set order_by_clause = var((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper(), default_order) -%}
{%- set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) -%}

{%- set variables = {
    'node_url': node_url,
    'model_quantum_state': model_quantum_state,
    'testing_limit': testing_limit,
    'new_build': new_build,
    'order_by_clause': order_by_clause,
    'uses_receipts_by_hash': uses_receipts_by_hash
} -%}

{{ return(variables) }}

{% endmacro %}  