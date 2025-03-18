{% macro boba_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'boba',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/boba/drpc/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['boba network', 'boba'],
        'DECODER_ABIS_EXPLORER_NAME': 'routescan',
        'DECODER_ABIS_EXPLORER_URL': 'https://api.routescan.io/v2/network/mainnet/evm/288/etherscan/api?module=contract&action=getabi&address='
    } %}
    
    {{ return(vars) }}
{% endmacro %} 