{% macro set_dynamic_fields(gold_model) %}
    {# Define all fields to check #}
    {% set all_fields = [
        {'gold_model': 'fact_blocks', 'field': 'baseFeePerGas', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'totalDifficulty', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'mixHash', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'blobGasUsed', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'excessBlobGas', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'parentBeaconBlockRoot', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'withdrawals', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'withdrawalsRoot', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'sendCount', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'sendRoot', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'author', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'requestsHash', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'blockGasCost', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'extDataHash', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'extDataGasUsed', 'source': 'blocks_fields'},
        {'gold_model': 'fact_blocks', 'field': 'blockExtraData', 'source': 'blocks_fields'},
        {'gold_model': 'fact_transactions', 'field': 'accessList', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'maxFeePerGas', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'maxPriorityFeePerGas', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'sourceHash', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'mint', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'ethValue', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'blobVersionedHashes', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'maxFeePerBlobGas', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'yParity', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'authorizationList', 'source': 'transactions_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1Fee', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1FeeScalar', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1GasUsed', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1GasPrice', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1BaseFeeScalar', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1BlobBaseFee', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1BlobBaseFeeScalar', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'gasUsedForL1', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'l1BlockNumber', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'blobGasUsed', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'blobGasPrice', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'tokenRatio', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'timeboosted', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'operatorFeeScalar', 'source': 'receipts_fields'},
        {'gold_model': 'fact_transactions', 'field': 'operatorFeeConstant', 'source': 'receipts_fields'}
    ] %}
    
    {# Filter fields based on the specified gold_model #}
    {% set fields_to_check = [] %}
    {% for item in all_fields %}
        {% if item.gold_model == gold_model %}
            {% do fields_to_check.append(item) %}
        {% endif %}
    {% endfor %}
    
    {# Make this query explicit to avoid case issues - define the field alias with quotes #}
    {% set rpc_settings_query %}
    SELECT 
        {% for item in fields_to_check %}
            array_contains('{{ item.field }}'::VARIANT, {{ item.source }}) as "{{ item.field }}" {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ ref('admin__fact_rpc_details') }}
    LIMIT 1
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}
    
    {% if execute %}
        
        {# Create return dictionary directly from column values #}
        {% set return_dict = {} %}
        
        {% if results.rows | length > 0 %}
            {% set row = results.rows[0] %}
            
            {# Directly map from column names to values #}
            {% for i in range(results.column_names | length) %}
                {% set col_name = results.column_names[i] %}
                {% set col_value = row[col_name] %}
                {% do return_dict.update({col_name: col_value}) %}
            {% endfor %}
        {% else %}
            {# No rows returned, set all fields to false #}
            {% for field in fields_to_check %}
                {% do return_dict.update({field.field: false}) %}
            {% endfor %}
        {% endif %}
        
        {# Return the constructed dictionary #}
        {% do return(return_dict) %}
    {% else %}
        {# In parsing mode, return empty dict #}
        {% do return({}) %}
    {% endif %}

{% endmacro %}