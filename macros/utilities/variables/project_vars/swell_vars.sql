{% macro swell_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'swell',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/swell/ankr/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Swell',
        'DECODER_ABIS_EXPLORER_NAME': 'SwellExplorer',
        'DECODER_ABIS_EXPLORER_URL': 'https://explorer.swellnetwork.io/api/v2/smart-contracts/'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 