{% macro ethereum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ethereum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/ethereum/quicknode/ethereum_mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'MAIN_SL_BLOCKS_PER_HOUR': 300,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Ethereum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'etherscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.etherscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/ethereum/block_explorers/etherscan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 