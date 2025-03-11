{% macro set_fact_transactions_vars() %}
    {# Query RPC settings for current chain #}
    {% set rpc_settings_query %}
        select 
            TX_HAS_ACCESS_LIST,
            TX_HAS_MAX_FEE_PER_GAS,
            TX_HAS_MAX_PRIORITY_FEE_PER_GAS,
            TX_HAS_BLOB_GAS_PRICE,
            TX_HAS_SOURCE_HASH,
            TX_HAS_MINT,
            TX_HAS_ETH_VALUE,
            TX_HAS_Y_PARITY,
            TX_HAS_L1_COLUMNS,
            TX_HAS_L1_TX_FEE_CALC,
            TX_HAS_BLOB_BASE_FEE,
            TX_HAS_EIP_1559
        from {{ ref('rpc__gold_settings') }}
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {% set row = results.rows[0] %}
        {% do return({
            'uses_access_list': row['TX_HAS_ACCESS_LIST'],
            'uses_max_fee_per_gas': row['TX_HAS_MAX_FEE_PER_GAS'],
            'uses_max_priority_fee_per_gas': row['TX_HAS_MAX_PRIORITY_FEE_PER_GAS'],
            'uses_blob_gas_price': row['TX_HAS_BLOB_GAS_PRICE'],
            'uses_source_hash': row['TX_HAS_SOURCE_HASH'],
            'uses_mint': row['TX_HAS_MINT'],
            'uses_eth_value': row['TX_HAS_ETH_VALUE'],
            'uses_y_parity': row['TX_HAS_Y_PARITY'],
            'uses_l1_columns': row['TX_HAS_L1_COLUMNS'],
            'uses_l1_tx_fee_calc': row['TX_HAS_L1_TX_FEE_CALC'],
            'uses_blob_base_fee': row['TX_HAS_BLOB_BASE_FEE'],
            'uses_eip_1559': row['TX_HAS_EIP_1559']
        }) %}
    {% else %}
        {% do return({}) %}
    {% endif %}
{% endmacro %}