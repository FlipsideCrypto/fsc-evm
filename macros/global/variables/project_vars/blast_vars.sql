{% macro blast_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'blast',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/blast/quicknode/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4300000000000000000000000000000000000004',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Blast',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'blastscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.blastscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/blast_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 