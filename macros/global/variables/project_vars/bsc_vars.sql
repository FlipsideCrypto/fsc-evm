{% macro bsc_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bsc',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/bsc/quicknode/mainnet',
        'GLOBAL_NODE_URL': '{service}/{Authentication}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'BNB',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['BNB','BNB Smart Chain (BEP20)'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'bscscan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.bscscan.com/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/bsc_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}

    {{ return(vars) }}
{% endmacro %} 