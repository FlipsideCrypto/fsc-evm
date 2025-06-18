{% macro optimism_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'optimism',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/optimism/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Optimism','optimistic-ethereum'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'opscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/optimism_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '20,50 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '10 5 * * *',
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'CURATED_STARGATE_TOKEN_MESSAGING_CONTRACT': '0xf1fcb4cbd57b67d683972a59b6a7b1e2e8bf27e6',
        'CURATED_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 