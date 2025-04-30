{% macro bob_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bob',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=bob&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'bob-network',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'GoBOB',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer-bob-mainnet-0.t.conduit.xyz/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '21,51 * * * *'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 