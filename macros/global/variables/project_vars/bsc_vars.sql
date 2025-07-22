{% macro bsc_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bsc',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/bsc/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WBNB',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'BNB',
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
        'CURATED_DEFI_RECENCY_EXCLUSION_LIST': ['level_finance-v1','woofi-v1','hashflow-v1'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x8909dc15e40173ff4699343b6eb8132c65e18ec6'
                },
                'v3': {
                    'uni_v3_pool_created': '0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ['0x5ca135cb8527d76e932f34b5145575f9d8cbe08e',
                    '0xf89e6ca06121b6d4370f4b196ae458e8b969a011']
                }
            },
            'kyberswap': {
                'v1': {
                    'dynamic': '0x878dfe971d44e9122048308301f540910bbd934c',
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'dodo': {
                'v1': {
                    'dodo_birth': '0xca459456a45e300aa7ef447dbb60f87cccb42828',
                    'proxy': ['0x8e4842d0570c85ba3805a9508dce7c6a458359d0',
                    '0x0596908263ef2724fbfbcafa1c983fcd7a629038',
                    '0x165ba87e882208100672b6c56f477ee42502c820',
                    '0xab623fbcaeb522046185051911209f5b2c2a2e1f']
                },
                'v2': {
                    'new_pool': ['0xafe0a75dffb395eaabd0a7e1bbbd0b11f8609eef',
                    '0xd9cac3d964327e47399aebd8e1e6dcc4c251daae',
                    '0x0fb9815938ad069bf90e14fe6c596c514bede767',
                    '0x790b4a80fb1094589a3c0efc8740aa9b0c1733fb']
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
                    '0xa98242820ebf3a405d265ccd22a4ea8f64afb281',
                    '0xb5574750a786a37e300a916974ecd63f93fc6754']
                },
                'v3': {
                    'create_pool': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
                }
            },
            'woofi': {
                'v1': {
                    'woo_swap': ['0xbf365ce9cfcb2d5855521985e351ba3bcf77fd3f',
                    '0x2217c57c91e3c6c55a90b4ca280f532d65590559']
                },
                'v2': {
                    'woo_router_swap': ['0xc90bfe9951a4efbf20aca5ecd9966b2bf8a01294',
                    '0x4f4fd4290c9bb49764701803af6445c5b03e8f06',
                    '0xcef5be73ae943b77f9bc08859367d923c030a269',
                    '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7'],
                    'woo_swap': ['0x59de3b49314bf5067719364a2cb43e8525ab93fa',
                    '0xec054126922a9a1918435c9072c32f1b60cb2b90',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                }
            },
            'biswap': {
                'v1': {
                    'uni_v2_pair_created': '0x858e3312ed3a876947ea49d572a7c42de08af7ee'
                }
            },
            'trader_joe': {
                'v1': {
                    'uni_v2_pair_created': '0x4f8bdc85e3eec5b9de67097c3f59b6db025d9986'
                },
                'v2': {
                    'lb_pair_created': '0x43646a8e839b2f2766392c1bf8f60f6e587b6960'
                },
                'v2.1': {
                    'lb_pair_created': '0x8e42f2f4101563bf679975178e880fd87d3efd4e'
                }
            },
            'level_finance': {
                'v1': {
                    'router': '0xa5abfb56a78d2bd4689b25b8a77fd49bb0675874'
                }
            },
            'pancakeswap': {
                'v1': {
                    'uni_v2_pair_created': '0xbcfccbde45ce874adcb698cc183debcf17952812'
                },
                'v2': {
                    'uni_v2_pair_created': ['0xca143ce32fe78f1f7019d7d551a6402fc5350c73',
                    '0x7b13d1d2a1fa28b16862ebac6e3c52fa9c8d753e',
                    '0x1f830fb91094a0e87c0a80150aa0af3805456090'],
                    'mm_router': '0xfeacb05b373f1a08e68235ba7fc92636b92ced01',
                    'ss_factory_1': '0x36bbb126e75351c0dfb651e39b38fe0bc436ffd2',
                    'ss_factory_2': '0x25a55f9f2279a54951133d503490342b50e5cd15'
                },
                'v3': {
                    'factory': '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
                }
            }
        },
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