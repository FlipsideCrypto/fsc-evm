{% macro set_fact_blocks_vars() %}
    {# Query RPC settings for current chain #}
    {% set fields_to_check = [
        'baseFeePerGas',
        'totalDifficulty',
        'mixHash',
        'blobGasUsed',
        'parentBeaconBlockRoot',
        'withdrawals'
        # Add new fields here in the future
    ] %}
    
    {% set rpc_settings_query %}
    select 
        {% for field in fields_to_check %}
        array_contains('{{ field }}'::VARIANT, blocks_fields) as fact_blocks_has_{{ field }},
        {% endfor %}
        1 as dummy  -- Prevents trailing comma issue
    from {{ ref('rpc__node_responses') }}
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {% set row = results.rows[0] %}
        {% set return_dict = {} %}
        {% for field in fields_to_check %}
            {% do return_dict.update({
                'fact_blocks_has_' ~ field: row['fact_blocks_has_' ~ field]
            }) %}
        {% endfor %}
        {% do return(return_dict) %}
    {% else %}
        {% do return({}) %}
    {% endif %}
{% endmacro %}