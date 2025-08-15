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
        'CURATED_DEFI_RECENCY_EXCLUSION_LIST': ['superchain_l2_standard_bridge-v1'],
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
        },
        'CURATED_DEFI_LENDING_CONTRACT_MAPPING': {
            'benqi': {
                'v1': {
                    'comp_v2_origin_from_address': '0x0f01756bc6183994d90773c8f22e3f44355ffa0e'
                }
            },
            'sonne': {
                'v1': {
                    'comp_v2_origin_from_address': '0xfb59ce8986943163f14c590755b29db2998f2322'
                }
            },
            'compound': {
                'v3': {
                    'comp_v3_origin_from_address': ['0x6103db328d4864dc16bd2f0ee1b9a92e3f87f915', '0x2501713a67a3dedde090e42759088a7ef37d4eab']
                }
            },
            'granary': {
                'v1': {
                    'aave_version_address': '0x8fd4af47e4e63d1d2d45582c3286b4bd9bb95dfe'
                }
            },
            'aave': {
                'v3': {
                    'aave_version_address': '0x794a61358d6845594f94dc1db02a252b5b4814ad'
                }
            },
            'morpho': {
                'v1': {
                    'morpho_blue_address': '0x857f3eefe8cbda3bc49367c996cd664a880d3042'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 
        
    } %}
    
    {{ return(vars) }}
{% endmacro %} 