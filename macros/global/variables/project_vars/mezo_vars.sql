{% macro mezo_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'mezo',
        'GLOBAL_NODE_PROVIDER': 'imperator',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/imperator/mezo/mainnet',
        'MAIN_SL_BLOCKS_PER_HOUR': 2000,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '22,52 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %}
