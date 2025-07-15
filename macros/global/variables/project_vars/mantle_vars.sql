{% macro mantle_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'mantle',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/mantle/quicknode/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WMNT',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['ETH', 'MNT'],
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': ['ethereum', 'mantle'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['mantle', 'Mantle'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'MantleScan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.mantlescan.xyz/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/mantle_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '24,54 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *',
        'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': '0x736a2ada5f4700d49da6b28a74c4a77cdb3e2994',
        'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': '0x5bcfc8ad38ee1da5f45d9795acadf57d37fec172',
        'CURATED_VERTEX_TOKEN_MAPPING': {
            'USDC': '0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9',
            'wMNT': '0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8',
            'METH': '0xcda86a272531e8640cd7f1a92c01839911b90bb0',
            'WETH': '0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111'
        },
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x41b491285a4f888f9f636cec8a363ab9770a0aef',
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
