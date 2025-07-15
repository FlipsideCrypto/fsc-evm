{% macro ink_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ink',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ink/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 3600,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'ink',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'InkOnChain',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer.inkonchain.com/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '9,39 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '50 5 * * *',
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x45f1a95a4d3f3836523f5c83673c797f4d4d263b',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0xca29f3a6f966cb2fc0de625f8f325c0c46dbe958',
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