{% macro avalanche_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'avalanche',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/avalanche/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'AVAX',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Avalanche', 'Avalanche C-Chain'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'snowtrace',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '10,40 * * * *',
        'CUSTOM_GHA_STREAMLINE_DEXALOT_CHAINHEAD_CRON': '50 * * * *',
        'CUSTOM_GHA_SCHEDULED_DEXALOT_MAIN_CRON': '5 * * * *',
        'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': '0x5db7f0fc871598ae424324477635cacce3f07bec',
        'CURATED_VERTEX_PROJECT_NAME': 'AVAX',
        'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': '0x7069798a5714c5833e36e70df8aefaac7cec9302',
        'CURATED_VERTEX_TOKEN_MAPPING': {
            'USDC': '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e',
            'WAVAX': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 