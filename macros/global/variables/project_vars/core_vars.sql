{% macro core_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'core',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=core&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x40375c92d9faf44d2f9db9bd9ba41a3317a2404f',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WCORE',
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
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'sushiswap': {
                'v2': {
                    'uni_v3_pool_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'corex': {
                'v1': {
                    'uni_v3_pool_created': '0x526190295afb6b8736b14e4b42744fbd95203a3a'
                }
            }
        },
        'CURATED_DEFI_DEX_DODO_PROXY_ADDRESSES': [],
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'layerzero': {
                'v2': {
                    'bridge': '0x1a44076050125825900e736c501f859c50fe728c'
                }
            },
            'meson': {
                'v1': {
                    'bridge': '0x25ab3efd52e6470681ce037cd546dc60726948d3'
                }
            },
            'stargate': {
                'v2': {
                    'bridge': '0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': '0x292fc50e4eb66c3f6514b9e402dbc25961824d62'
                }
            },
            'gaszip_lz': {
                'v2': {
                    'send_deposits': '0x26da582889f59eaae9da1f063be0140cd93e6a4f',
                    'packet_sent': '0x1a44076050125825900e736c501f859c50fe728c',
                    'send_uln': '0x0bcac336466ef7f1e0b5c184aab2867c108331af'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 