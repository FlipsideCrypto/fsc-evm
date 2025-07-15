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
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c',
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': ''
                },
                'v3': {
                    'uni_v3_pool_created': ''
                }
            },
            'sushiswap': {
                'v1': {
                    'deploy_pool': []
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ''
                }
            },
            'kyberswap': {
                'v1': {
                    'static': ''
                },
                'v2': {
                    'elastic': ''
                }
            },
            'dodo': {
                'v2': {
                    'deployer': '',
                    'new_pool': []
                }
            },
            'hashflow': {
                'v1': {
                    'create': []
                },
                'v3': {
                    'create_pool': ''
                }
            },
            'woofi': {
                'v1': {
                    'woo_router_swap': '',
                    'woo_swap': []
                },
                'v2': {
                    'woo_router_swap': ''
                }
            },
            'curve': {
                'v1': {
                    'deployer': []
                }
            },
            'balancer': {
                'v1': {
                    'vault': ''
                }
            }
        },
        'CURATED_DEFI_DEX_DODO_PROXY_ADDRESSES': [],
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v1': {
                    'funds_deposited': ''
                },
                'v3': {
                    'funds_deposited': ''
                }
            },
            'allbridge': {
                'v1': {
                    'sent': ''
                },
                'v2': {
                    'tokens_sent': '',
                    'lp': []
                }
            },
            'axelar': {
                'v1': {
                    'gateway': [],
                    'gas_service': '',
                    'squid_router': '',
                    'burn': ''
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': ''
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': ''
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': ''
                }
            },
            'eywa': {
                'v1': {
                    'portal': []
                }
            },
            'layerzero': {
                'v2': {
                    'bridge': ''
                }
            },
            'meson': {
                'v1': {
                    'bridge': ''
                }
            },
            'multichain': {
                'v7': {
                    'router': ''
                }
            },
            'stargate': {
                'v1': {
                    'factory': '',
                    'bridge': ''
                },
                'v2': {
                    'bridge': ''
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': ''
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': '',
                    'token_bridge_swap': ''
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': ''
                }
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': '',
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': ''
    } %}
    
    {{ return(vars) }}
{% endmacro %} 