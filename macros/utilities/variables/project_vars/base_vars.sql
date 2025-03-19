{% macro base_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'base',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/base/quicknode/base_mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Base',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'basescan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.basescan.org/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/base_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': 900,
        'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': 450
    } %}
    
    {{ return(vars) }}
{% endmacro %}
