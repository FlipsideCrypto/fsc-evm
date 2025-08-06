{% macro ink_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'ink',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/ink/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 3600,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'ink',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'InkOnChain',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer.inkonchain.com/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '9,39 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '50 5 * * *',
        'CURATED_DEFI_RECENCY_EXCLUSION_LIST': ['inkswap-v1'],
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': ['0x458c5d5b75ccba22651d2c5b61cb1ea1e0b0f95d', '0xfe57a6ba1951f69ae2ed4abe23e0f095df500c04']
                },
                'v3': {
                    'uni_v3_pool_created': '0x640887a9ba3a9c53ed27d0f7e8246a4f933f3424'
                }
            },
            'camelot': {
                'v2': {
                    'uni_v2_pair_created': '0x31832f2a97fd20664d76cc421207669b55ce4bc0'
                }
            },
            'squidswap': {
                'v1': {
                    'uni_v2_pair_created': '0x63b54dbbd2dabf89d5c536746e534711f6094199'
                }
            },
            'inkswap': {
                'v1': {
                    'uni_v2_pair_created': '0xbd5b41358a6601924f1fd708af1535a671f530a9'
                }
            },
            'velodrome': {
                'v2': {
                    'factory': '0x31832f2a97fd20664d76cc421207669b55ce4bc0'
                },
                'v3': {
                    'superchain_slipstream': '0x04625b046c69577efc40e6c0bb83cdbafab5a55f'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v3': {
                    'funds_deposited': '0xef684c38f94f48775959ecf2012d7e864ffb9dd4'
                }
            },
            'everclear': {
                'v1': {
                    'bridge': '0xa05a3380889115bf313f1db9d5f335157be4d816'
                }
            },
            'layerzero': {
                'v2': {
                    'bridge': '0xca29f3a6f966cb2fc0de625f8f325c0c46dbe958'
                }
            },
            'l2_standard_bridge': {
                'v1': {
                    'bridge': '0x4200000000000000000000000000000000000010'
                }
            },
            'stargate': {
                'v2': {
                    'bridge': '0x45f1a95a4d3f3836523f5c83673c797f4d4d263b'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 