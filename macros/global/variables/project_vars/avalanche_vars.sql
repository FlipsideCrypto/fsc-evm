{% macro avalanche_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'avalanche',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/avalanche/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WAVAX',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'AVAX',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'AVAX',
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Avalanche', 'Avalanche C-Chain'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'snowtrace',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_REALTIME_PRODUCER_BATCH_SIZE': 50,
        'DECODER_SL_CONTRACT_ABIS_REALTIME_WORKER_BATCH_SIZE': 50,
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '10,40 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '10 5 * * *',
        'CUSTOM_GHA_STREAMLINE_DEXALOT_CHAINHEAD_CRON': '50 * * * *',
        'CUSTOM_GHA_SCHEDULED_DEXALOT_MAIN_CRON': '5 * * * *',
        'CURATED_DEFI_RECENCY_EXCLUSION_LIST': ['allbridge-v1','multichain-v7','platypus-v1','gmx-v1','woofi-v1','hashflow-v1'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'arena_trade': {
                'v1': {
                    'uni_v2_pair_created': '0xf16784dcaf838a3e16bef7711a62d12413c39bd1'
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
                    'swap_executed': '0xeed3c159f3a96ab8d41c8b9ca49ee1e5071a7cdd'
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ['0x5ca135cb8527d76e932f34b5145575f9d8cbe08e',
                    '0xf77ca9b635898980fb219b4f4605c50e4ba58aff']
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0x05fb0089bec6d00b2f01f4096eb0e0488c79cd91',
                    '0x7677bf119654d1fbcb46cb9014949bf16180b6ae']
                },
                'v3': {
                    'create_pool': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
                }
            },
            'kyberswap': {
                'v1': {
                    'dynamic': '0x10908c875d865c66f271f5d3949848971c9595c9',
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x9e5a52f57b3038f1b8eee45f28b3c1967e22799c'
                },
                'v3': {
                    'uni_v3_pool_created': '0x740b1c1de25031c31ff4fc9a62f554a55cdc1bad'
                }
            },
            'woofi': {
                'v1': {
                    'woo_router_swap': '0x5aa6a4e96a9129562e2fc06660d07feddaaf7854',
                    'woo_swap': ['0xf8ce0d043891b62c55380fb1efbfb4f186153d96',
                    '0x1df3009c57a8b143c6246149f00b090bce3b8f88',
                    '0x3b3e4b4741e91af52d0e9ad8660573e951c88524',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                },
                'v2': {
                    'woo_router_swap': ['0xc22fbb3133df781e6c25ea6acebe2d2bb8cea2f9',
                    '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7']
                }
            },
            'pangolin': {
                'v1': {
                    'uni_v2_pair_created': '0xefa94de7a4656d787667c749f7e1223d71e9fd88'
                }
            },
            'gmx': {
                'v1': {
                    'swap': '0x9ab2de34a33fb459b538c43f251eb825645e8595'
                }
            },
            'pharaoh': {
                'v1': {
                    'swap': '0xaaa16c016bf556fcd620328f0759252e29b1ab57'
                },
                'v2': {
                    'uni_v3_pool_created': '0xaaa32926fce6be95ea2c51cb4fcb60836d320c42'
                }
            },
            'platypus': {
                'v1': {
                    'deployer': '0x416a7989a964c9ed60257b064efc3a30fe6bf2ee'
                }
            },
            'trader_joe': {
                'v1': {
                    'uni_v2_pair_created': '0x9ad6c38be94206ca50bb0d90783181662f0cfa10'
                },
                'v2': {
                    'lb_pair_created': '0x6e77932a92582f504ff6c4bdbcef7da6c198aeef'
                },
                'v2.1': {
                    'lb_pair_created': '0x8e42f2f4101563bf679975178e880fd87d3efd4e'
                },
                'v2.2': {
                    'lb_pair_created': '0xb43120c4745967fa9b93e79c149e66b0f2d6fe0c'
                }
            }
        },
        'CURATED_DEFI_DEX_DEXALOT_DEST_CHAIN_ID': 43114,
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'allbridge': {
                'v1': {
                    'sent': '0xbbbd1bbb4f9b936c3604906d7592a644071de884'
                },
                'v2': {
                    'tokens_sent': '0x9068e1c28941d0a680197cc03be8afe27ccaeea9',
                    'lp': ['0x2d2f460d7a1e7a4fcc4ddab599451480728b5784','0xe827352a0552ffc835c181ab5bf1d7794038ec9f']
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0x5029c0eff6c34351a0cec334542cdb22c7928f78',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0xf4c7e640eda248ef95972845a62bdc74237805db'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': ['0xbb7684cc5408f4dd0921e5c2cadd547b8f1ad573',
                    '0x9b36f165bab9ebe611d491180418d8de4b8f3a1f',
                    '0xef3c714c9425a8f3697a9c969dc1af30ba82e5d4']
                }
            },
            'circle_cctp': {
                'v1': {
                    'deposit': '0x6b25532e1060ce10cc3b0a99e5683b91bfde6982'
                },
                'v2': {
                    'deposit': '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            },
            'everclear': {
                'v1': {
                    'bridge': '0x9aa2ecad5c77dfcb4f34893993f313ec4a370460'
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
                    'factory': '0x808d7c71ad2ba3fa531b068a2417c63106bc0949',
                    'bridge': '0x9d1b1669c73b033dfe47ae5a0164ab96df25b944'
                },
                'v2': {
                    'bridge': '0x17e450be3ba9557f2378e20d64ad417e59ef9a34'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': '0xe75c7e85fe6add07077467064ad15847e6ba9877'
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': ['0xc4133e23c783af2c732c06677b98b905b5c65c46',
                    '0xc05e61d0e7a63d27546389b7ad62fdff5a91aace'],
                    'token_bridge_swap': '0xc05e61d0e7a63d27546389b7ad62fdff5a91aace'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x0e082f06ff657d94310cb8ce8b0d9a04541d8052'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': 'avalanche c-chain',
        'CURATED_DEFI_LENDING_CONTRACT_MAPPING': {
            'benqi': {
                'v1': {
                    'comp_v2_origin_from_address': ['0x5423819b3b5bb38b0e9e9e59f22f9034e2d8819b',
                    '0x0df1a01ade3cd67ccc11d89f2859a0de514cd679',
                    '0xf799c20563218190424c3aec6022ce9faf588eb7',
                    '0x0cf89de760b234b82e475d609a6de8ec48c68677',
                    '0xfb45e03b83ad113cd0d4e697354a6a9be6decc55']
                }
            },
            'joe_lend': {
                'v1': {
                    'comp_v2_origin_from_address': ['0x5d3e4c0fe11e0ae4c32f0ff74b4544c49538ac61',
                    '0x72c5456d731fdd9d3480f997226a631231de61cc']
                }
            },
            'aave': {
                'v3': {
                    'aave_version_address': '0x794a61358d6845594f94dc1db02a252b5b4814ad'
                }
            },
            'euler': {
                'v1': {
                    'euler_origin_to_address': '0x7f53e2755eb3c43824e162f7f6f087832b9c9df6'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 