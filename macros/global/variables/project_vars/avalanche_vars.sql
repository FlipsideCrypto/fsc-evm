{% macro avalanche_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'avalanche',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/avalanche/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WAVAX',
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
        'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': '0x5db7f0fc871598ae424324477635cacce3f07bec',
        'CURATED_VERTEX_PROJECT_NAME': 'AVAX',
        'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': '0x7069798a5714c5833e36e70df8aefaac7cec9302',
        'CURATED_VERTEX_TOKEN_MAPPING': {
            'USDC': '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e',
            'WAVAX': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
        },
        'CURATED_DEFI_DEX_POOLS_CONTRACT_MAPPING': {
            'arena-trade': {
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
            }
        },
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x17e450be3ba9557f2378e20d64ad417e59ef9a34',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c'
    } %}
    
    {{ return(vars) }}
{% endmacro %} 