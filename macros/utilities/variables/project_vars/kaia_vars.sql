{% macro kaia_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'arbitrum',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/arbitrum/quicknode/mainnet',
        'GLOBAL_NODE_URL': '{service}/{Authentication}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        'MAIN_SL_BLOCKS_PER_HOUR': 14200,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Arbitrum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'arbiscan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.arbiscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/arbitrum_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 