{% macro set_fact_transactions_vars() %}
    {# Define all fields to check #}
    {% set fields_to_check = [
        {'field': 'accessList', 'source': 'transactions_fields'},
        {'field': 'maxFeePerGas', 'source': 'transactions_fields'},
        {'field': 'maxPriorityFeePerGas', 'source': 'transactions_fields'},
        {'field': 'blobGasPrice', 'source': 'transactions_fields'},
        {'field': 'sourceHash', 'source': 'transactions_fields'},
        {'field': 'mint', 'source': 'transactions_fields'},
        {'field': 'ethValue', 'source': 'transactions_fields'},
        {'field': 'yParity', 'source': 'transactions_fields'},
        {'field': 'l1Fee', 'source': 'receipts_fields'},
        {'field': 'l1FeeScalar', 'source': 'receipts_fields'},
        {'field': 'l1BlobBaseFee', 'source': 'receipts_fields'}
    ] %}
    
    {# Query RPC settings for current chain #}
    {% set rpc_settings_query %}
    select 
        {% for item in fields_to_check %}
            array_contains('{{ item.field }}'::VARIANT, {{ item.source }}) as fact_transactions_has_{{ item.field }},
        {% endfor %}
        1 as dummy  -- Prevents trailing comma issue
    from {{ ref('rpc__node_responses') }}
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {% set row = results.rows[0] %}
        {% set return_dict = {} %}
        
        {% for item in fields_to_check %}
            {% do return_dict.update({
                'fact_transactions_has_' ~ item.field: row['fact_transactions_has_' ~ item.field]
            }) %}
        {% endfor %}
        
        {% do return(return_dict) %}
    {% else %}
        {% do return({}) %}
    {% endif %}
{% endmacro %}