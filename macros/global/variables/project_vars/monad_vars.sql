{% macro monad_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'monad',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/monad/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WMON',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'MON',
        'MAIN_SL_BLOCKS_PER_HOUR': 10000,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'MON',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'monad',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'monad',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '24,54 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '45 5 * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 