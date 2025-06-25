{% macro mezo_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'mezo',
        'GLOBAL_NODE_PROVIDER': 'imperator',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/imperator/mezo/mainnet',
        'MAIN_SL_BLOCKS_PER_HOUR': 1000,
        'MAIN_CORE_RECEIPTS_BY_HASH_ENABLED': true,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'BTC',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'bitcoin',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Mezo',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'MezoExplorer',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.explorer.mezo.org/api/v2/smart-contracts/',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '22,52 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %}
