{% macro ink_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ink',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ink/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 3600,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'ink',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'InkOnChain',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer.inkonchain.com/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '9,39 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '50 5 * * *',
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x45f1a95a4d3f3836523f5c83673c797f4d4d263b',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0xca29f3a6f966cb2fc0de625f8f325c0c46dbe958'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 