{% macro ethereum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ethereum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ethereum/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
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
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': 'ethereum mainnet'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 