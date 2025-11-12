{% macro somnia_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'somnia',
        'GLOBAL_NODE_PROVIDER': 'flipside',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'vault/prod/evm/flipside/somnia/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x046ede9564a72571df6f5e44d0405360c0f4dcab',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WSOMI',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'SOMI',
        'MAIN_SL_BLOCKS_PER_HOUR': 36000,
        'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_BLOCKS_TRANSACTIONS_REALTIME_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_BLOCKS_TRANSACTIONS_HISTORY_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_RECEIPTS_REALTIME_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_RECEIPTS_REALTIME_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_RECEIPTS_REALTIME_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_RECEIPTS_HISTORY_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_RECEIPTS_HISTORY_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_RECEIPTS_HISTORY_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_TRACES_HISTORY_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_TRACES_HISTORY_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_TRACES_HISTORY_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_CONFIRM_BLOCKS_REALTIME_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_CONFIRM_BLOCKS_REALTIME_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_CONFIRM_BLOCKS_REALTIME_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_SL_CONFIRM_BLOCKS_HISTORY_PRODUCER_BATCH_SIZE': 36000,
        'MAIN_SL_CONFIRM_BLOCKS_HISTORY_WORKER_BATCH_SIZE': 360,
        'MAIN_SL_CONFIRM_BLOCKS_HISTORY_ASYNC_CONCURRENT_REQUESTS': 25,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'SOMI',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'somnia',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'somnia',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'somnia',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://mainnet.somnia.w3us.site/api/v2/smart-contracts/',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '9,39 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '50 5 * * *',
        'CURATED_DEFI_DEX_SWAPS_RECENCY_EXCLUSION_LIST': ['uniswap-v3','sushiswap-v1','sushiswap-v2'],
        'CURATED_DEFI_DEX_LP_ACTIONS_RECENCY_EXCLUSION_LIST': ['uniswap-v3','sushiswap-v1','sushiswap-v2'],
        'CURATED_DEFI_BRIDGE_RECENCY_EXCLUSION_LIST': ['celer_cbridge-v1','symbiosis-v1','synapse-v1','l2_standard_bridge-v1'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'quickswap': {
                'v4': {
                    'pool': '0x0ccff3d02a3a200263ec4e0fdb5e60a56721b8ae'
                }
            },
            'somnex': {
                'v2': {
                    'uni_v2_pair_created': '0xafd71143fb155058e96527b07695d93223747ed1'
                },
                'v3': {
                    'uni_v3_pool_created': '0xdd594374a0fa18cd074ed61288f392ed1ccbaffd'
                }
            },
            'somnia_exchange': {
                'v2': {
                    'uni_v2_pair_created': '0x6c4853c97b981aa848c2b56f160a73a46b5dccd4'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'layerzero': {
                'v2': {
                    'bridge': '0x6f475642a6e85809b1c36fa62763669b1b48dd5b'
                }
            },
            'stargate': {
                'v2': {
                    'bridge': '0x78add880a697070c1e765ac44d65323a0dcce913'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 