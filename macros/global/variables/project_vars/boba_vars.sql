{% macro boba_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'boba',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=boba-eth&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['boba network', 'boba'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'routescan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.routescan.io/v2/network/mainnet/evm/288/etherscan/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '3,33 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '25 5 * * *',
        'DECODER_SL_CONTRACT_ABIS_REALTIME_PRODUCER_BATCH_SIZE': 50,
        'DECODER_SL_CONTRACT_ABIS_REALTIME_WORKER_BATCH_SIZE': 50,
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'CURATED_DEFI_BRIDGE_RECENCY_EXCLUSION_LIST': ['meson-v1','symbiosis-v1'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v3': {
                    'uni_v3_pool_created': '0xffcd7aed9c627e82a765c3247d562239507f6f1b'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                },
                'v2': {
                    'uni_v3_pool_created': '0x0be808376ecb75a5cf9bb6d237d16cd37893d904'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 