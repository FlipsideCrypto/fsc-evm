{% macro optimism_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'optimism',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/optimism/quicknode/mainnet',
        'GLOBAL_NODE_URL': '{service}/{Authentication}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Optimism','optimistic-ethereum'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'opscan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/optimism_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 