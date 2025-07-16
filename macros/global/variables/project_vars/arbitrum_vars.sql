{% macro arbitrum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'arbitrum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/arbitrum/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 14200,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Arbitrum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'arbiscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.arbiscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/arbitrum_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '5,35 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *',
        'MAIN_CORE_TRACES_ARB_MODE': true,
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0xf1d7cc64fb4452f05c498126312ebe29f30fbcf9'
                },
                'v3': {
                    'uni_v3_pool_created': '0x1f98431c8ad98523631ae4a59f267346ea31f984'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                },
                'v2': {
                    'uni_v3_pool_created': '0x1af415a1eba07a4986a52b6f2e7de7003d82231e'
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ['0x5ca135cb8527d76e932f34b5145575f9d8cbe08e',
                    '0x8374a74a728f06bea6b7259c68aa7bbb732bfead']
                }
            },
            'kyberswap': {
                'v1': {
                    'dynamic': '0xd9bfe9979e9ca4b2fe84ba5d4cf963bbcb376974',
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': ['0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a',
                    '0xc7a590291e07b9fe9e64b86c58fd8fc764308c4a']
                }
            },
            'dodo': {
                'v1': {
                    'dodo_birth': '0xbcc3401e16c25eaf4d3fed632ce3288503883b1f'
                },
                'v2': {
                    'new_pool': ['0xa6cf3d163358af376ec5e8b7cc5e102a05fde63d',
                    '0xddb13e6dd168e1a68dc2285cb212078ae10394a9',
                    '0x7b07164ecfaf0f0d85dfc062bc205a4674c75aa0',
                    '0x1506b54a1c0ea1b2f4a84866ec5776f7f6e7f0b1',
                    '0x9340e3296121507318874ce9c04afb4492af0284',
                    '0xc8fe2440744dcd733246a4db14093664defd5a53',
                    '0xda4c4411c55b0785e501332354a036c04833b72b']
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0xe43632337d3f9a52ffd098fe71a57cc5961c041f',
                    '0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
                    '0x75fb2ab4d5b0de8b1a1acdc9124887d35d459084']
                },
                'v3': {
                    'create_pool': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
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
                    '0x1f79f8a65e02f8a137ce7f79c038cc44332df448',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                },
                'v2': {
                    'woo_router_swap': ['0xcdfd61a8303beb5c8dd2a6d02df8d228ce15b9f3',
                    '0x9aed3a8896a85fe9a8cac52c9b402d092b629a30',
                    '0xd2635bc7e4e4f63b2892ed80d0b0f9dff7eda899',
                    '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7']
                },
                'v3': {
                    'woo_router_swap': '0xb130a49065178465931d4f887056328cea5d723f'
                }
            },
            'curve': {
                'v1': {
                    'deployer': ['0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c',
                    '0xbabe61887f1de2713c6f97e567623453d3c79f67',
                    '0xb17b674d9c5cb2e441f8e196a2f048a81355d031']
                }
            },
            'balancer': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            },
            'dexalot': {
                'v1': {
                    'swap_executed': '0x010224949cca211fb5ddfedd28dc8bf9d2990368'
                }
            },
            'gmx': {
                'v1': {
                    'swap': '0x489ee077994b6658eafa855c308275ead8097c4a'
                },
                'v2': {
                    'swap': '0xc8ee91a54287db53897056e12d9819156d3822fb'
                }
            },
            'ramses': {
                'v2': {
                    'uni_v3_pool_created': '0xaa2cd7477c451e703f3b9ba5663334914763edf8'
                }
            },
            'sparta': {
                'v1': {
                    'uni_v2_pair_created': '0xfe8ec10fe07a6a6f4a2584f8cd9fe232930eaf55'
                }
            },
            'trader_joe': {
                'v1': {
                    'uni_v2_pair_created': '0xae4ec9901c3076d0ddbe76a520f9e90a6227acb7'
                },
                'v2': {
                    'lb_pair_created': '0x1886d09c9ade0c5db822d85d21678db67b6c2982'
                },
                'v2.1': {
                    'lb_pair_created': ['0x8e42f2f4101563bf679975178e880fd87d3efd4e',
                    '0xee0616a2deaa5331e2047bc61e0b588195a49cea',
                    '0x8597db3ba8de6baadeda8cba4dac653e24a0e57b']
                },
                'v2.2': {
                    'lb_pair_created': '0xb43120c4745967fa9b93e79c149e66b0f2d6fe0c'
                }
            },
            'camelot': {
                'v1': {
                    'uni_v2_pair_created': '0x6eccab422d763ac031210895c81787e87b43a652'
                },
                'v2': {
                    'pool': ['0xd490f2f6990c0291597fd1247651b4e0dcf684dd',
                    '0x1a3c9b1d2f0529d97f2afc5136cc23e58f1fd35b']
                }
            },
            'maverick': {
                'v2': {
                    'factory': '0x0a7e848aca42d879ef06507fca0e7b33a0a63c1e'
                }
            },
            'pancakeswap': {
                'v3': {
                    'factory': '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
                }
            },
            'zyberswap': {
                'v1': {
                    'uni_v2_pair_created': '0xac2ee06a14c52570ef3b9812ed240bce359772e7'
                },
                'v2': {
                    'pool': '0x9c2abd632771b433e5e7507bcaa41ca3b25d8544'
                }
            },
        },
        'CURATED_DEFI_DEX_DEXALOT_DEST_CHAIN_ID': 42161,
        'CURATED_DEFI_DEX_DODO_PROXY_ADDRESSES': ['0xd5a7e197bace1f3b26e2760321d6ce06ad07281a','0x8ab2d334ce64b50be9ab04184f7ccba2a6bb6391'],
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v1': {
                    'funds_deposited': '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a'
                },
                'v3': {
                    'funds_deposited': '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a'
                }
            },
            'allbridge': {
                'v2': {
                    'tokens_sent': '0x9ce3447b58d58e8602b7306316a5ff011b92d189',
                    'lp': ['0x47235cb71107cc66b12af6f8b8a9260ea38472c7','0x690e66fc0f8be8964d40e55ede6aebdfcb8a21df']
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
                    'router': '0x141fa059441e0ca23ce184b6a78bafd2a517dde8'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': ['0xdd90e5e87a2081dcf0391920868ebc2ffb81a1af',
                    '0x1619de6b6b20ed217a58d00f37b9d47c7663feca']
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
            'multichain': {
                'v7': {
                    'router': '0x1633d66ca91ce4d81f63ea047b7b19beb92df7f3'
                }
            },
            'stargate': {
                'v1': {
                    'factory': '0x55bdb4164d28fbaf0898e0ef14a589ac09ac9970',
                    'bridge': '0x352d8275aae3e0c2404d9f68f6cee084b5beb3dd'
                },
                'v2': {
                    'bridge': '0x19cfce47ed54a88614648dc3f19a5980097007dd'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': ['0x0425841529882628880fbd228ac90606e0c2e09a',
                    '0x01a3c8e513b758ebb011f7afaf6c37616c9c24d9']
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': '0x6f4e8eba4d337f874ab57478acc2cb5bacdc19c9',
                    'token_bridge_swap': '0x6f4e8eba4d337f874ab57478acc2cb5bacdc19c9'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x0b2402144bb366a632d14b83f244d2e0e21bd39c'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '0x25fb92e505f752f730cad0bd4fa17ece4a384266',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 