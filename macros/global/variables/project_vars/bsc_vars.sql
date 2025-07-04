{% macro bsc_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bsc',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/bsc/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        'MAIN_SL_BLOCKS_PER_HOUR': 4800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'BNB',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['BNB','BNB Smart Chain (BEP20)'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'bscscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.bscscan.com/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/bsc_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '15,45 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '30 5 * * *',
        'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': 4800,
        'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': 600,
        'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS': 50,
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x6e3d884c96d640526f273c61dfcf08915ebd7e2b',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 