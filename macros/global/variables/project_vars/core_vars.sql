{% macro core_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'core',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=core&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x40375c92d9faf44d2f9db9bd9ba41a3317a2404f',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_SL_TRANSACTIONS_PER_BLOCK': 50,
        'MAIN_CORE_RECEIPTS_BY_HASH_ENABLED': true,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'CORE',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ["Core", "core"],
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '23,53 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '35 5 * * *',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'CoreScan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://openapi.coredao.org/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/core_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED': true,
        'MAIN_CORE_GOLD_TRACES_TEST_ERROR_THRESHOLD': 10,
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 