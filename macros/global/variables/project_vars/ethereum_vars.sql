{% macro ethereum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ethereum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ethereum/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 300,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Ethereum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'etherscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.etherscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/ethereum/block_explorers/etherscan',
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '0,30 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '40 5 * * *',
        'CUSTOM_GHA_TEST_BEACON_CRON': '5 9 * * *',
        'CUSTOM_GHA_STREAMLINE_READS_CRON': '40 1-23/2 * * *',
        'CUSTOM_GHA_STREAMLINE_BEACON_CRON': '55 */1 * * *',
        'CUSTOM_GHA_SCHEDULED_BEACON_CRON': '10 */2 * * *',
        'CUSTOM_GHA_NFT_READS_CRON': '0 * * * *',
        'CUSTOM_GHA_NFT_LIST_CRON': '0 0,12 * * *',
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'MAIN_SL_TOKEN_READS_BRONZE_TABLE_ENABLED': true,
        'CURATED_DEFI_RECENCY_EXCLUSION_LIST': ['synthetix-v1','pancakeswap-v2','hashflow-v1','across-v1','zora_bridge-v1','near_rainbow_bridge-v1',
        'ronin_axie_bridge-v1','multichain-v7','hop-v1','axie_infinity-v2'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f'
                },
                'v3': {
                    'uni_v3_pool_created': '0x1f98431c8ad98523631ae4a59f267346ea31f984'
                },
                'v4': {
                    'factory': '0x000000000004444c5dc75cb358380d2e3de08a90'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac'
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ['0xb076b06f669e682609fb4a8c6646d2619717be4b',
                    '0x43ec799eadd63848443e2347c49f5f52e8fe0f6f']
                }
            },
            'kyberswap': {
                'v1': {
                    'dynamic': '0x833e4083b7ae46cea85695c4f7ed25cdad8886de',
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'dodo': {
                'v1': {
                    'deployer': ['0x5e5a7b76462e4bdf83aa98795644281bdba80b88',
                    '0x17dbfa501f2f376d092fa69d3223a09bba4efdf7'],
                    'dodo_birth': ['0xbd337924f000dceb119153d4d3b1744b22364d25',
                    '0xe1b5d7a770cb1b40c859a52696e7e3dd1c57b0ba',
                    '0x3a97247df274a17c59a3bd12735ea3fcdfb49950'],
                    'proxy': ['0x91e1c84ba8786b1fae2570202f0126c0b88f6ec7',
                    '0x9b64c81ba54ea51e1f6b7fefb3cff8aa6f1e2a09',
                    '0xe6aafa1c45d9d0c64686c1f1d17b9fe9c7dab05b',
                    '0xe55154d09265b18ac7cdac6e646672a5460389a1']
                },
                'v2': {
                    'new_pool': ['0x95e887adf9eaa22cc1c6e3cb7f07adc95b4b25a8',
                    '0x5336ede8f971339f6c0e304c66ba16f1296a2fbe',
                    '0x6b4fa0bc61eddc928e0df9c7f01e407bfcd3e5ef',
                    '0xb5dc5e183c2acf02ab879a8569ab4edaf147d537',
                    '0x6fddb76c93299d985f4d3fc7ac468f9a168577a4',
                    '0x79887f65f83bdf15bcc8736b5e5bcdb48fb8fe13',
                    '0x72d220ce168c4f361dd4dee5d826a01ad8598f6c']
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
                    '0xc11a1e6fde432df9467d6d1a5454b54a63b86c8c',
                    '0x596d32f9b7c1f2e73f5071c66b5e336e27d00da4']
                },
                'v3': {
                    'create_pool': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
                }
            },
            'woofi': {
                'v2': {
                    'woo_router_swap': ['0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7',
                    '0x044c08639bd59beb4f6ec52c0da6cd47283534e8']
                }
            },
            'curve': {
                'v1': {
                    'deployer': ['0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0xf18056bbd320e96a48e3fbf8bc061322531aac99',
                    '0xc447fcaf1def19a583f97b3620627bf69c05b5fb',
                    '0xb9fc157394af804a3578134a6585c0dc9cc990d4',
                    '0xfd6f33a0509ec67defc500755322abd9df1bd5b8',
                    '0xbf7d65d769e82e7b862df338223263ba33f72623',
                    '0xa6df4fcb1ca559155a678e9aff5de3f210c0ff84',
                    '0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
                    '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c',
                    '0x3e0139ce3533a42a7d342841aee69ab2bfee1d51',
                    '0xbabe61887f1de2713c6f97e567623453d3c79f67',
                    '0x7f7abe23fc1ad4884b726229ceaafb1179e9c9cf',
                    '0x4f8846ae9380b90d2e71d5e3d042dff3e7ebb40d',
                    '0x0c0e5f2ff0ff18a3be9b835635039256dc4b4963']
                }
            },
            'balancer': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            },
            'maverick': {
                'v1': {
                    'swap': ['0x4faf448121bf2985b991c0261dd356a9803b3cae',
                    '0xa5ebd82503c72299073657957f41b9cea6c0a43a']
                }
            },
            'pancakeswap': {
                'v1': {
                    'uni_v2_pair_created': '0x1097053fd2ea711dad45caccc45eff7548fcb362'
                },
                'v2': {
                    'mm_router': '0x9ca2a439810524250e543ba8fb6e88578af242bc'
                },
                'v3': {
                    'factory': '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
                }
            },
            'shibaswap': {
                'v1': {
                    'uni_v2_pair_created': '0x115934131916c8b277dd010ee02de363c09d037c'
                }
            },
            'synthetix': {
                'v1': {
                    'synth_exchange': ['0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f',
                    '0xc011a72400e58ecd99ee497cf89e3775d4bd732f']
                }
            },
            'trader_joe': {
                'v2.1': {
                    'lb_pair_created': '0xdc8d77b69155c7e68a95a4fb0f06a71ff90b943a'
                }
            },
            'verse': {
                'v1': {
                    'uni_v2_pair_created': '0xee3e9e46e34a27dc755a63e2849c9913ee1a06e2'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v1': {
                    'funds_deposited': '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
                },
                'v3': {
                    'funds_deposited': '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5'
                }
            },
            'allbridge': {
                'v1': {
                    'sent': '0xbbbd1bbb4f9b936c3604906d7592a644071de884'
                },
                'v2': {
                    'tokens_sent': '0x609c690e8f7d68a59885c9132e812eebdaaf0c9e',
                    'lp': ['0x7dbf07ad92ed4e26d5511b4f285508ebf174135d',
                    '0xa7062bba94c91d565ae33b893ab5dfaf1fc57c4d']
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0x4f4495243837681061c4743b74b3eedf548d56a5',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x80226fc0ee2b096224eeac085bb9a8cba1146f7d'
                }
            },
            'circle_cctp': {
                'v1': {
                    'deposit': '0xbd3fa81b58ba92a82136038b25adec7066af3155'
                },
                'v2': {
                    'deposit': '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': '0x5427fefa711eff984124bfbb1ab6fbf5e3da1820'
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            },
            'everclear': {
                'v1': {
                    'bridge': '0xa05a3380889115bf313f1db9d5f335157be4d816'
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
                    'router': ['0x1633d66ca91ce4d81f63ea047b7b19beb92df7f3',
                    '0x93251f98acb0c83904320737aec091bce287f8f5']
                }
            },
            'stargate': {
                'v1': {
                    'factory': '0x06d538690af257da524f25d0cd52fd85b1c2173e',
                    'bridge': '0x296f55f8fb28e498b858d0bcda06d955b2cb3f97'
                },
                'v2': {
                    'bridge': '0x6d6620efa72948c5f68a3c8646d58c00d3f4a980'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': ['0xb80fdaa74dda763a8a158ba85798d373a5e84d84',
                    '0xb8f275fbf7a959f4bce59999a2ef122a099e81a8']
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': '0x2796317b0ff8538f253012862c06787adfb8ceb6',
                    'token_bridge_swap': '0x2796317b0ff8538f253012862c06787adfb8ceb6'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x3ee18b2214aff97000d974cf647e7c347e8fa585'
                }
            },
            'axie_infinity': {
                'v2': {
                    'deposit_requested': '0x64192819ac13ef72bf6b5ae239ac672b43a9af08'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': 'ethereum mainnet',
        'CURATED_DEFI_BRIDGE_HOP_L1_CONTRACTS': ['0xb8901acb165ed027e32754e0ffe830802919727f',
        '0x236fe0ffa7118505f2a1c35a039f6a219308b1a7']
    } %}
    
    {{ return(vars) }}
{% endmacro %} 