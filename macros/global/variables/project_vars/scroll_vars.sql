{% macro scroll_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'scroll',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/scroll/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x5300000000000000000000000000000000000004',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '12,42 * * * *',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['ETH', 'SCROLL'],
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': ['ethereum', 'scroll'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['scroll', 'Scroll'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'ScrollScan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.scrollscan.com/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/scroll_scan'
    } %}
    
    {{ return(vars) }}
{% endmacro %}
