{% macro run_daily_search_optimization() %}
  {% set tables_to_optimize = [
    -- Core package models
    {'table': 'core__dim_contract_abis', 'schema': 'core', 'optimization': 'EQUALITY(contract_address,bytecode), SUBSTRING(contract_address,bytecode)'},
    {'table': 'core__dim_contracts', 'schema': 'core', 'optimization': 'EQUALITY(address, symbol, name), SUBSTRING(address, symbol, name)'},
    {'table': 'core__dim_labels', 'schema': 'core', 'optimization': 'EQUALITY(address, label_type, label_subtype, address_name, label), SUBSTRING(address, label_type, label_subtype, address_name, label)'},
    {'table': 'core__ez_decoded_event_logs', 'schema': 'core', 'optimization': 'EQUALITY(ez_decoded_event_logs_id, contract_name, contract_address)'},
    {'table': 'core__ez_native_transfers', 'schema': 'core', 'optimization': 'EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)'},
    {'table': 'core__ez_token_transfers', 'schema': 'core', 'optimization': 'EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)'},
    {'table': 'core__fact_event_logs', 'schema': 'core', 'optimization': 'EQUALITY(block_number,tx_hash,contract_address,origin_from_address,origin_to_address,origin_function_signature,topic_0)'},
    {'table': 'core__fact_traces', 'schema': 'core', 'optimization': 'EQUALITY(block_number,tx_hash,from_address,to_address,trace_address,type,origin_from_address,origin_to_address,origin_function_signature), SUBSTRING(input,output,type,trace_address)'},
    {'table': 'core__fact_transactions', 'schema': 'core', 'optimization': 'EQUALITY(block_number,tx_hash,from_address,to_address,origin_function_signature), SUBSTRING(input_data)'},
    {'table': 'nft__ez_nft_transfers', 'schema': 'core', 'optimization': 'EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)'},
    
    -- Silver layer models
    {'table': 'silver__blocks', 'schema': 'silver', 'optimization': 'equality(block_number)'},
    {'table': 'silver__confirm_blocks', 'schema': 'silver', 'optimization': 'equality(block_number)'},
    {'table': 'silver__created_contracts', 'schema': 'silver', 'optimization': 'EQUALITY(block_timestamp, tx_hash, created_contract_address, creator_address), SUBSTRING(created_contract_address, creator_address)'},
    {'table': 'silver__proxies', 'schema': 'silver', 'optimization': ''},
    {'table': 'silver__relevant_contracts', 'schema': 'silver', 'optimization': 'equality(contract_address)'},
    {'table': 'silver__traces', 'schema': 'silver', 'optimization': 'equality(block_number)'},
    {'table': 'silver__transactions', 'schema': 'silver', 'optimization': 'equality(block_number)'},
    
    -- Prices package models
    {'table': 'price__dim_asset_metadata', 'schema': 'core', 'optimization': 'EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)'},
    {'table': 'price__ez_asset_metadata', 'schema': 'core', 'optimization': 'EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)'},
    {'table': 'price__ez_prices_hourly', 'schema': 'core', 'optimization': 'EQUALITY(token_address, symbol, name),SUBSTRING(token_address, symbol, name)'},
    {'table': 'price__fact_prices_ohlc_hourly', 'schema': 'core', 'optimization': 'EQUALITY(asset_id),SUBSTRING(asset_id)'},
    
    -- Admin models
    {'table': 'admin__number_sequence', 'schema': 'admin', 'optimization': 'equality(_id)'},
    
    -- Streamline complete models
    {'table': 'streamline__blocks_complete', 'schema': 'bronze', 'optimization': 'equality(block_number)'},
    {'table': 'streamline__confirm_blocks_complete', 'schema': 'bronze', 'optimization': 'equality(block_number)'},
    {'table': 'streamline__receipts_complete', 'schema': 'bronze', 'optimization': 'equality(block_number)'},
    {'table': 'streamline__receipts_by_hash_complete', 'schema': 'bronze', 'optimization': 'equality(block_number, tx_hash)'},
    {'table': 'streamline__traces_complete', 'schema': 'bronze', 'optimization': 'equality(block_number)'},
    {'table': 'streamline__transactions_complete', 'schema': 'bronze', 'optimization': 'equality(block_number)'},
    
    -- Decoder package models
    {'table': 'silver__abis', 'schema': 'silver_decoder', 'optimization': 'EQUALITY(contract_address,abi_hash,bytecode), SUBSTRING(contract_address,abi_hash,bytecode)'},
    {'table': 'silver__complete_event_abis', 'schema': 'silver_decoder', 'optimization': ''},
    {'table': 'silver__flat_event_abis', 'schema': 'silver_decoder', 'optimization': 'EQUALITY (contract_address)'},
    {'table': 'silver__verified_abis', 'schema': 'silver_decoder', 'optimization': 'equality(contract_address)'},
    {'table': 'streamline__complete_contract_abis', 'schema': 'bronze_decoder', 'optimization': 'equality(complete_contract_abis_id, contract_address)'},
    {'table': 'streamline__decoded_logs_complete', 'schema': 'bronze_decoder', 'optimization': 'equality(_log_id)'},
    {'table': 'streamline__token_reads_complete', 'schema': 'bronze', 'optimization': 'equality(complete_token_reads_id, contract_address)'},
    
    -- Curated package models
    {'table': 'silver_bridge__complete_bridge_activity', 'schema': 'silver_defi', 'optimization': 'EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain_id, destination_chain, token_address, token_symbol), SUBSTRING(origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain, token_address, token_symbol)'},
    {'table': 'silver_dex__complete_dex_liquidity_pools', 'schema': 'silver_defi', 'optimization': 'EQUALITY(tx_hash, contract_address, pool_address, pool_name, tokens, symbols), SUBSTRING(pool_address, pool_name, tokens, symbols)'},
    {'table': 'silver_dex__complete_dex_swaps', 'schema': 'silver_defi', 'optimization': 'EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, pool_name, event_name, sender, tx_to, token_in, token_out, symbol_in, symbol_out), SUBSTRING(origin_function_signature, pool_name, event_name, sender, tx_to, token_in, token_out, symbol_in, symbol_out)'},
    
    -- Balances package models
    {'table': 'streamline__state_tracer_complete', 'schema': 'bronze_balances', 'optimization': 'equality(block_number)'},
    
    -- Legacy models
    {'table': 'bronze_api__contract_abis', 'schema': 'bronze_decoder', 'optimization': 'equality(contract_address)'}
  ] %}

  {% for table_info in tables_to_optimize %}
    {% if table_info.optimization != '' %}
      {% set database = target.database %}
      {% set schema = target.schema + '_' + table_info.schema if table_info.schema != 'core' else target.schema %}
      {% set table_name = database + '.' + schema + '.' + table_info.table %}
      
      {% set sql %}
        ALTER TABLE {{ table_name }} ADD SEARCH OPTIMIZATION ON {{ table_info.optimization }}
      {% endset %}
      
      {% do log("Optimizing search for: " ~ table_name, info=true) %}
      {% do run_query(sql) %}
    {% endif %}
  {% endfor %}

  {{ log("Daily search optimization complete for " ~ tables_to_optimize|length ~ " tables", info=true) }}
  
  SELECT 
    '{{ tables_to_optimize|length }}' as tables_optimized,
    SYSDATE() as completed_at
{% endmacro %} 