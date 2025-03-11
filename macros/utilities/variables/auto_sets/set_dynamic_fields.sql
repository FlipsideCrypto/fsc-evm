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
            array_contains('{{ item.field }}'::VARIANT, {{ item.source }}) as {{ item.field|lower }},
        {% endfor %}
        1 as dummy  -- Prevents trailing comma issue
    from {{ ref('rpc__node_responses') }}
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {# Get the result row and create return dictionary #}
        {% set return_dict = {} %}
        
        {% if results.rows | length > 0 %}
            {% set row = results.rows[0] %}
            
            {# Process each field and explicitly handle boolean values #}
            {% for field in fields_to_check %}
                {% set field_name = field.field %}
                {% set column_name = field.field|lower %}
                
                {# Check if the column exists in the results #}
                {% if column_name in row %}
                    {% set field_value = row[column_name] %}
                    
                    {# Handle boolean values explicitly #}
                    {% if field_value is sameas true %}
                        {% do return_dict.update({field_name: true}) %}
                    {% elif field_value is sameas false %}
                        {% do return_dict.update({field_name: false}) %}
                    {% else %}
                        {# If not a boolean, use the value as is #}
                        {% do return_dict.update({field_name: field_value}) %}
                    {% endif %}
                {% else %}
                    {# If column doesn't exist in results, default to false #}
                    {% do return_dict.update({field_name: false}) %}
                {% endif %}
            {% endfor %}
        {% else %}
            {# No rows returned, set all fields to false #}
            {% for field in fields_to_check %}
                {% do return_dict.update({field.field: false}) %}
            {% endfor %}
        {% endif %}
        
        {# Log the final dictionary for debugging #}
        {{ log('Return dictionary for ' ~ gold_model ~ ': ' ~ return_dict, info=True) }}
        
        {# Return the constructed dictionary #}
        {% do return(return_dict) %}
    {% else %}
        {# In parsing mode, return empty dict #}
        {% do return({}) %}
    {% endif %}

{% endmacro %}