{% macro monad_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'monad',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_URL': "{URL}",
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/monad/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x3bd359c1119da7da1d913d1c4d2b7c461115433a',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WMON',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'MON',
        'MAIN_SL_BLOCKS_PER_HOUR': 10000,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'MON',
        'CURATED_START_TIMESTAMP': '2025-11-18 00:00:00',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'monad',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'monad',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'etherscan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/evm/etherscan/pro_plus',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.etherscan.io/v2/api?apikey={KEY}&chainid=143&module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL_SUFFIX': '&tag=latest',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '24,54 * * * *',
        'BALANCES_SL_DAILY_REALTIME_LOOKBACK_DAYS': -2,
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x182a927119d56008d921126764bf884221b10f59'
                },
                'v3': {
                    'uni_v3_pool_created': '0x204faca1764b154221e35c0d20abb3c525710498'
                },
                'v4': {
                    'factory': '0x188d586ddcf52439676ca21a244753fa19f9ea8e'
                }
            },
            'pancakeswap': {
                'v2': {
                    'uni_v2_pair_created': '0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e'
                },
                'v3': {
                    'factory': '0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865'
                }
            },
            'curve': {
                'v1': {
                    'deployer': [
                        '0x8271e06e5887fe5ba05234f5315c19f3ec90e8ad',
                        '0x6e28493348446503db04a49621d8e6c9a40015fb',
                        '0xe7fbd704b938cb8fe26313c3464d4b7b7348c88c'
                    ]
                }
            },
            'octoswap': {
                'v1': {
                    'uni_v2_pair_created': '0xce104732685b9d7b2f07a09d828f6b19786cda32'
                },
                'v2': {
                    'uni_v3_pool_created': '0x30db57a29acf3641dfc3885af2e5f1f5a408d9cb'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'layerzero': {
                'v2': {
                    'bridge': '0x6f475642a6e85809b1c36fa62763669b1b48dd5b'
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0xe432150cce91c13a887f7d836923d5597add8e31',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712'
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x0b2719cda2f10595369e6673cea3ee2edfa13ba7'
                }
            },
            'circle_cctp': {
                'v2': {
                    'deposit': '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x33566fe5976aaa420f3d5c64996641fc3858cadb'
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            }
        },
        'CURATED_DEFI_LENDING_CONTRACT_MAPPING': {
            'morpho': {
                'v1': {
                    'morpho_blue_address': '0xd5d960e8c380b724a48ac59e2dff1b2cb4a1eaee'
                }
            },
            'neverland': {
                'v1': {
                    'aave_version_address': '0x80f00661b13cc5f6ccd3885be7b4c9c67545d585'
                }
            }
        }
    } %}
    
    {{ return(vars) }}

{% endmacro %} 