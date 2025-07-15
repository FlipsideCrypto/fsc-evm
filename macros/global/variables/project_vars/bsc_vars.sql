{% macro bsc_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bsc',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/bsc/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WBNB',
        'MAIN_SL_BLOCKS_PER_HOUR': 4800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'BNB',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['BNB','BNB Smart Chain (BEP20)'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'bscscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.bscscan.com/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/bsc_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '15,45 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '30 5 * * *',
        'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': 4800,
        'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': 600,
        'MAIN_SL_TRACES_REALTIME_ASYNC_CONCURRENT_REQUESTS': 50,
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
                'v3': {
                    'funds_deposited': '0x4e8e101924ede233c13e2d8622dc8aed2872d505'
                }
            },
            'allbridge': {
                'v1': {
                    'sent': '0xbbbd1bbb4f9b936c3604906d7592a644071de884'
                },
                'v2': {
                    'tokens_sent': '0x3c4fa639c8d7e65c603145adad8bd12f2358312f',
                    'lp': ['0xf833afa46fcd100e62365a0fdb0734b7c4537811',
                    '0x8033d5b454ee4758e4bd1d37a49009c1a81d8b10']
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0x304acf330bbe08d1e512eefaa92f6a57871fd895',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x34b03cb9086d7d758ac55af71584f81a598759fe'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': ['0x5d96d4287d1ff115ee50fac0526cf43ecf79bfc6',
                    '0x9b36f165bab9ebe611d491180418d8de4b8f3a1f',
                    '0x265b25e22bcd7f10a5bd6e6410f10537cc7567e8',
                    '0xdd90e5e87a2081dcf0391920868ebc2ffb81a1af']
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            },
            'eywa': {
                'v2': {
                    'portal': ['0xece9cf6a8f2768a3b8b65060925b646afeaa5167',
                    '0xac8f44ceca92b2a4b30360e5bd3043850a0ffcbe',
                    '0xbf0b5d561b986809924f88099c4ff0e6bcce60c9']
                }
            },
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
                'v1': {
                    'factory': '0xe7ec689f432f29383f217e36e680b5c855051f25',
                    'bridge': '0x6694340fc020c5e6b96567843da2df01b2ce1eb6'
                },
                'v2': {
                    'bridge': '0x6e3d884c96d640526f273c61dfcf08915ebd7e2b'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': ['0xb91d3060c90aac7c4c706aef2b37997b3b2a1dcf',
                    '0x5aa5f7f84ed0e5db0a4a85c3947ea16b53352fd4']
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': ['0x0efc29e196da2e81afe96edd041bedcdf9e74893',
                    '0xd123f70ae324d34a9e76b67a27bf77593ba8749f'],
                    'token_bridge_swap': ['0x0efc29e196da2e81afe96edd041bedcdf9e74893',
                    '0xd123f70ae324d34a9e76b67a27bf77593ba8749f']
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0xb6f6d86a8f9879a9c87f643768d9efc38c1da6e7'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': 'bnb smart chain mainnet'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 