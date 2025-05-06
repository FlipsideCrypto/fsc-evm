{% macro avalanche_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'avalanche',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/avalanche/c_chain/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'AVAX',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Avalanche', 'Avalanche C-Chain'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'snowtrace',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '10,40 * * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 