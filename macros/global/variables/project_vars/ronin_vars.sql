{% macro ronin_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ronin',
        'GLOBAL_NODE_PROVIDER': 'tatum',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/ronin/tatum/mainnet_new',
        'GLOBAL_NODE_URL': "{api_url}",
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xe514d9deb7966c8be0ca922de8a064264ea6bcd4',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'RON',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'ronin',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'RoninChain',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer-kintsugi.roninchain.com/v2/2020/contract/',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL_SUFFIX': '/abi',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_SL_CHAINHEAD_DELAY_MINUTES': 6
    } %}
    
    {{ return(vars) }}
{% endmacro %} 