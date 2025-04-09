{% macro gnosis_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'gnosis',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/gnosis/quicknode/mainnet',
        'GLOBAL_NODE_URL': '{service}/{Authentication}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['GNO','XDAI'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Gnosis','Gnosis Chain'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'gnosisscan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.gnosisscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/gnosis_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 