{% macro set_dynamic_fields(gold_model) %}
    {# Define all fields to check #}
    {% set all_fields = [
        {'gold_model': 'fact_blocks', 'field': 'baseFeePerGas', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'totalDifficulty', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'mixHash', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'blobGasUsed', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'parentBeaconBlockRoot', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'withdrawals', 'source': 'blocks_fields'},
        {'gold_model': 'fact_transactions', 'field': 'accessList', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'maxFeePerGas', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'maxPriorityFeePerGas', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'blobGasPrice', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'sourceHash', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'mint', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'ethValue', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'yParity', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1Fee', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1FeeScalar', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1BlobBaseFee', 'source': 'receipts_fields'}
    ] %}
    
    {# Filter fields based on the specified gold_model #}
    {% set fields_to_check = [] %}
    {% for item in all_fields %}
        {% if item.gold_model == gold_model %}
            {% do fields_to_check.append(item) %}
        {% endif %}
    {% endfor %}
    
    {# Query RPC settings for current chain #}
    {% set rpc_settings_query %}
    select 
        {% for item in fields_to_check %}
            array_contains('{{ item.field }}'::VARIANT, {{ item.source }}) as field_{{ loop.index }},
        {% endfor %}
        1 as dummy  -- Prevents trailing comma issue
    from {{ ref('rpc__node_responses') }}
    limit 1
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {% set row = results.rows[0] %}
        
        {# Create a dictionary with field values #}
        {% set field_dict = {} %}
        
        {% for i in range(fields_to_check | length) %}
            {% set field_name = fields_to_check[i].field %}
            {% set field_value = row['field_' ~ (i+1)] %}
            {% set _ = field_dict.update({field_name: field_value}) %}
            {{ log('Field ' ~ field_name ~ ' = ' ~ field_value, info=true) }}
        {% endfor %}
        
        {{ return(field_dict) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}