{% macro swell_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'swell',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=swell&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Swell',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'SwellExplorer',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer.swellnetwork.io/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '17,47 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '30 5 * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 