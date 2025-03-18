{% macro polygon_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'polygon',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/polygon/quicknode/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        'MAIN_SL_BLOCKS_PER_HOUR': 1700,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['MATIC','POL']
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Polygon',
        'DECODER_ABIS_EXPLORER_NAME': 'polygonscan',
        'DECODER_ABIS_EXPLORER_URL': 'https://api.polygonscan.com/api?module=contract&action=getabi&address=',
        'DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH': 'Vault/prod/block_explorers/polygon_scan',
        'MAIN_SL_CHAINHEAD_DELAY_MINUTES': 10
    } %}
    
    {{ return(vars) }}
{% endmacro %} 