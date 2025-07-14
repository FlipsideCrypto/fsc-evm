{% macro optimism_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'optimism',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/optimism/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
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
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x0c3c1c532f1e39edf36be9fe0be1410313e074bf'
                },
                'v3': {
                    'uni_v3_pool_created': '0x1f98431c8ad98523631ae4a59f267346ea31f984'
                }
            },
            'sushiswap': {
                'v1': {
                    'deploy_pool': ['0xcaabdd9cf4b61813d4a52f980d6bc1b713fe66f5',
                    '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506']
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': '0x67a1412d2d6cbf211bb71f8e851b4393b491b10f'
                }
            },
            'kyberswap': {
                'v1': {
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'dodo': {
                'v2': {
                    'deployer': '0x386a28709a31532d4f68b06fd28a27e4ea378364'
                    'new_pool': ['0xdb9c53f2ced34875685b607c97a61a65da2f30a8',
                    '0x1f83858cd6d0ae7a08ab1fd977c06dabece6d711',
                    '0x2b800dc6270726f7e2266ce8cd5a3f8436fe0b40']
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0x246d44b1221e44930b207a1a7e235b616c465158',
                    '0x63ae536fec0b57bdeb1fd6a893191b4239f61bff']
                },
                'v3': {
                    'create_pool': '0x6d551f4d999fac0984eb75b2b230ba7e7651bde7'
                }
            },
            'woofi': {
                'v1': {
                    'woo_router_swap': '0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024'
                    'woo_swap': ['0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                },
                'v2': {
                    'woo_router_swap': '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7'
                }
            },
            'curve': {
                'v1': {
                    'deployer': ['0x2db0e83599a91b508ac268a6197b8b14f5e72840',
                    '0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c']
                }
            },
            'beethoven_x': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            },
            'synthetix': {
                'v1': {
                    'synth_swap': '0x8700daec35af8ff88c16bdf0418774cb3d7599b4'
                }
            },
            'velodrome': {
                'v1': {
                    'pair_created': '0x25cbddb98b35ab1ff77413456b31ec81a6b6b746'
                },
                'v2': {
                    'factory': '0xf1046053aa5682b4f9a81b5481394da16be5ff5a',
                    'converter': '0x585af0b397ac42dbef7f18395426bf878634f18d'
                }
            }
        },
        'CURATED_DEFI_DEX_DODO_PROXY_ADDRESSES': ['0xdd0951b69bc0cf9d39111e5037685fb573204c86','0x169ae3d5acc90f0895790f6321ee81cb040e8a6b'],
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v1': {
                    'funds_deposited': '0x6f26bf09b1c792e3228e5467807a900a503c0281'
                },
                'v3': {
                    'funds_deposited': '0x6f26bf09b1c792e3228e5467807a900a503c0281'
                }
            },
            'allbridge': {
                'v1': {
                    'sent': '0x97e5bf5068ea6a9604ee25851e6c9780ff50d5ab'
                },
                'v2': {
                    'lp': ['0xb24a05d54fcacfe1fc00c59209470d4cafb0deea','0x3b96f88b2b9eb87964b852874d41b633e0f1f68f']
                }
            },
            'axelar': {
                'v1': {
                    'gateway': ['0xe19bb3b98f7727c520c757b8a00753eb47358b14',
                    '0xe432150cce91c13a887f7d836923d5597add8e31'],
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x3206695cae29952f4b0c22a169725a865bc8ce0f'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': '0x9d39fc627a6d9d9f8c831c16995b209548cc3401'
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            },
            'eywa': {
                'v1': {
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
            'multichain': {
                'v7': {
                    'router': '0x1633d66ca91ce4d81f63ea047b7b19beb92df7f3'
                }
            },
            'stargate': {
                'v1': {
                    'factory': '0xe3b53af74a4bf62ae5511055290838050bf764df',
                    'bridge': '0x701a95707a0290ac8b90b3719e8ee5b210360883'
                },
                'v2': {
                    'bridge': '0xf1fcb4cbd57b67d683972a59b6a7b1e2e8bf27e6'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': '0x292fc50e4eb66c3f6514b9e402dbc25961824d62'
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': '0xaf41a65f786339e7911f4acdad6bd49426f2dc6b'
                    'token_bridge_swap': '0xaf41a65f786339e7911f4acdad6bd49426f2dc6b'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x1d68124e65fafc907325e3edbf8c4d84499daa8b'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '0x03d7f750777ec48d39d080b020d83eb2cb4e3547',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 