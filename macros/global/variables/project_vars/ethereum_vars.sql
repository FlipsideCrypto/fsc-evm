{% macro ethereum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ethereum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ethereum/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'MAIN_SL_BLOCKS_PER_HOUR': 300,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Ethereum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'etherscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.etherscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/ethereum/block_explorers/etherscan',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '0,30 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '40 5 * * *',
        'CUSTOM_GHA_TEST_BEACON_CRON': '5 9 * * *',
        'CUSTOM_GHA_STREAMLINE_READS_CRON': '40 1-23/2 * * *',
        'CUSTOM_GHA_STREAMLINE_BEACON_CRON': '55 */1 * * *',
        'CUSTOM_GHA_SCHEDULED_BEACON_CRON': '10 */2 * * *',
        'CUSTOM_GHA_NFT_READS_CRON': '0 * * * *',
        'CUSTOM_GHA_NFT_LIST_CRON': '0 0,12 * * *',
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 