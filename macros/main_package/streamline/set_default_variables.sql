{% macro set_default_variables_streamline(package_name,model_name, model_type) %}

{%- set node_url = get_var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}
{%- set node_secret_path = get_var('GLOBAL_NODE_VAULT_PATH', '') -%}
{%- set model_quantum_state = get_var((package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type ~ '_quantum_state').upper(), 'streamline') -%}
{%- set testing_limit = get_var((package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}
{%- set new_build = get_var((package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type ~ '_new_build_enabled').upper(), false) -%}
{%- set default_order = 'ORDER BY partition_key DESC, block_number DESC' if model_type.lower() == 'realtime' 
    else 'ORDER BY partition_key ASC, block_number ASC' -%}
{%- set order_by_clause = get_var((package_name ~ '_SL_' ~ model_name ~ '_' ~ model_type ~ '_order_by_clause').upper(), default_order) -%}
{%- set uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}

{%- set variables = {
    'node_url': node_url,
    'node_secret_path': node_secret_path,
    'model_quantum_state': model_quantum_state,
    'testing_limit': testing_limit,
    'new_build': new_build,
    'order_by_clause': order_by_clause,
    'uses_receipts_by_hash': uses_receipts_by_hash
} -%}

{{ return(variables) }}

{% endmacro %}  

{% macro set_default_variables_bronze(source_name, model_type) %}

{%- set partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)" -%}
{%- set partition_join_key = 'partition_key' -%}
{%- set block_number = true -%}
{%- set balances = false -%}
{%- set uses_receipts_by_hash = get_var('MAIN_CORE_RECEIPTS_BY_HASH_ENABLED', false) -%}

{%- set variables = {
    'partition_function': partition_function,
    'partition_join_key': partition_join_key,
    'block_number': block_number,
    'balances': balances,
    'uses_receipts_by_hash': uses_receipts_by_hash
} -%}

{{ return(variables) }}

{% endmacro %} 