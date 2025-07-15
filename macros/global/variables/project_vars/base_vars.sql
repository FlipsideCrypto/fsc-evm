{% macro base_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'base',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/base/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['base', 'Base'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'basescan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.basescan.org/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/base_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_SL_TRACES_REALTIME_PRODUCER_BATCH_SIZE': 900,
        'MAIN_SL_TRACES_REALTIME_WORKER_BATCH_SIZE': 450,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '15,45 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '15 5 * * *',
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x8909dc15e40173ff4699343b6eb8132c65e18ec6'
                },
                'v3': {
                    'uni_v3_pool_created': '0x33128a8fc17869897dce68ed026d694621f6fdfd'
                }
            },
            'sushiswap': {
                'v2': {
                    'uni_v3_pool_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'woofi': {
                'v1': {
                    'woo_swap': ['0x39d361e66798155813b907a70d6c2e3fdafb0877',
                    '0xc04362cf21e6285e295240e30c056511df224cf4',
                    '0x86b1742a1d7c963d3e8985829d722725316abf0a',
                    '0xeff23b4be1091b53205e35f3afcd9c7182bf3062',
                    '0xb89a33227876aef02a7ebd594af9973aece2f521',
                    '0x8693f9701d6db361fe9cc15bc455ef4366e39ae0',
                    '0xb130a49065178465931d4f887056328cea5d723f',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                },
                'v2': {
                    'woo_router_swap': ['0xcdfd61a8303beb5c8dd2a6d02df8d228ce15b9f3',
                    '0x9aed3a8896a85fe9a8cac52c9b402d092b629a30',
                    '0xd2635bc7e4e4f63b2892ed80d0b0f9dff7eda899',
                    '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7']
                },
                'v3': {
                    'woo_router_swap': '0x27425e9fb6a9a625e8484cfd9620851d1fa322e5'
                }
            },
            'curve': {
                'v1': {
                    'deployer': ['0xa5961898870943c68037f6848d2d866ed2016bcb',
                    '0x3093f9b57a428f3eb6285a589cb35bea6e78c336',
                    '0x5ef72230578b3e399e6c6f4f6360edf95e83bbfd']
                }
            },
            'balancer': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            },
            'alienbase': {
                'v1': {
                    'uni_v2_pair_created': '0x3e84d913803b02a4a7f027165e8ca42c14c0fde7'
                }
            },
            'baseswap': {
                'v1': {
                    'uni_v2_pair_created': '0xfda619b6d20975be80a10332cd39b9a4b0faa8bb'
                },
                'v2': {
                    'uni_v3_pool_created': '0x38015d05f4fec8afe15d7cc0386a126574e8077b'
                }
            },
            'dexalot': {
                'v1': {
                    'swap_executed': '0x1fd108cf42a59c635bd4703b8dbc8a741ff834be'
                }
            },
            'swapbased': {
                'v1': {
                    'uni_v2_pair_created': '0x04c9f118d21e8b767d2e50c946f0cc9f6c367300'
                }
            }
        },
        'CURATED_DEFI_DEX_DEXALOT_DEST_CHAIN_ID': 8453,
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v1': {
                    'funds_deposited': '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
                },
                'v3': {
                    'funds_deposited': '0x09aea4b2242abc8bb4bb78d537a67a245a7bec64'
                }
            },
            'allbridge': {
                'v2': {
                    'tokens_sent': '0x001e3f136c2f804854581da55ad7660a2b35def7',
                    'lp': '0xda6bb1ec3baba68b26bea0508d6f81c9ec5e96d5'
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0xe432150cce91c13a887f7d836923d5597add8e31',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x881e3a65b4d4a04dd529061dd0071cf975f58bcd'
                }
            },
            'circle_cctp': {
                'v1': {
                    'deposit': '0x1682ae6375c4e4a97e4b583bc394c861a46d8962'
                },
                'v2': {
                    'deposit': '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': '0x7d43aabc515c356145049227cee54b608342c0ad'
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
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
                    'factory': '0xaf5191b0de278c7286d6c7cc6ab6bb8a73ba2cd6',
                    'bridge': '0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398'
                },
                'v2': {
                    'bridge': '0x5634c4a5fed09819e3c46d86a965dd9447d86e47'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': '0xee981b2459331ad268cc63ce6167b446af4161f8'
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': '0xf07d1c752fab503e47fef309bf14fbdd3e867089',
                    'token_bridge_swap': '0xf07d1c752fab503e47fef309bf14fbdd3e867089'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x8d2de8d2f73f1f4cab472ac9a881c9b123c79627'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '0xe22d2bedb3eca35e6397e0c6d62857094aa26f52',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
    } %}
    
    {{ return(vars) }}
{% endmacro %}
