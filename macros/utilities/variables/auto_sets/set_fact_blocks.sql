{% macro set_fact_blocks_vars() %}
    {# Query RPC settings for current chain #}
    {% set rpc_settings_query %}
    select 
        BLOCKS_HAS_BASE_FEE,
        BLOCKS_HAS_TOTAL_DIFFICULTY,
        BLOCKS_HAS_MIX_HASH,
        BLOCKS_HAS_BLOB_GAS_USED,
        BLOCKS_HAS_PARENT_BEACON_BLOCK_ROOT,
        BLOCKS_HAS_WITHDRAWALS
    from {{ ref('rpc__gold_settings') }}
    {% endset %}

    {% set results = run_query(rpc_settings_query) %}

    {% if execute %}
        {% set row = results.rows[0] %}
        {% do return({
            'uses_base_fee': row['BLOCKS_HAS_BASE_FEE'],
            'uses_total_difficulty': row['BLOCKS_HAS_TOTAL_DIFFICULTY'],
            'uses_mix_hash': row['BLOCKS_HAS_MIX_HASH'],
            'uses_blob_gas_used': row['BLOCKS_HAS_BLOB_GAS_USED'],
            'uses_parent_beacon_block_root': row['BLOCKS_HAS_PARENT_BEACON_BLOCK_ROOT'],
            'uses_withdrawals': row['BLOCKS_HAS_WITHDRAWALS']
        }) %}
    {% else %}
        {% do return({}) %}
    {% endif %}
{% endmacro %}