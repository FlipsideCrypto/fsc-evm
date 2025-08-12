{% macro somnia_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'somnia',
        'GLOBAL_NODE_PROVIDER': 'flipside',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'vault/prod/evm/flipside/somnia/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': '',
        'GLOBAL_NATIVE_ASSET_SYMBOL': '',
        'MAIN_SL_BLOCKS_PER_HOUR': 36000,
        'MAIN_PRICES_NATIVE_SYMBOLS': '',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': '',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'somnia',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'somnia',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': '',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '9,39 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '50 5 * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 