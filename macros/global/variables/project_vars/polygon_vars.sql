{% macro polygon_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'polygon',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/polygon/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WPOL',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'POL',
        'MAIN_SL_BLOCKS_PER_HOUR': 1700,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['MATIC','POL'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Polygon',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'polygonscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.polygonscan.com/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/polygon_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_SL_CHAINHEAD_DELAY_MINUTES': 10,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '25,55 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '15 5 * * *',
        'MAIN_CORE_BRONZE_TOKEN_READS_LIMIT': 30,
        'MAIN_CORE_BRONZE_TOKEN_READS_BATCHED_ENABLED': true,
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true,
        'CURATED_DEFI_LENDING_RECENCY_EXCLUSION_LIST': ['aave-v2'],
        'CURATED_DEFI_DEX_SWAPS_RECENCY_EXCLUSION_LIST': ['woofi-v1','hashflow-v1'],
        'CURATED_DEFI_DEX_LP_ACTIONS_RECENCY_EXCLUSION_LIST': ['kyberswap-v2','curve-v1'],
        'CURATED_DEFI_BRIDGE_RECENCY_EXCLUSION_LIST': ['hop-v1','multichain-v7','symbiosis-v1','across-v2'],
        'CURATED_DEFI_TVL_MORPHO_BLUE_ADDRESS': '0x1bf0c2541f820e775182832f06c0b7fc27a25f67',
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'uni_v2_pair_created': '0x9e5a52f57b3038f1b8eee45f28b3c1967e22799c'
                },
                'v3': {
                    'uni_v3_pool_created': '0x1f98431c8ad98523631ae4a59f267346ea31f984'
                },
                'v4': {
                    'factory': '0x67366782805870060151383f4bbff9dab53e5cd6'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'quickswap': {
                'v1': {
                    'uni_v2_pair_created': '0x5757371414417b8c6caad45baef941abc7d3ab32'
                },
                'v2': {
                    'pool': '0x411b0facc3489691f28ad58c47006af5e3ab3a28'
                }
            },
            'fraxswap': {
                'v1': {
                    'uni_v2_pair_created': ['0xc2544a32872a91f4a553b404c6950e89de901fdb',
                    '0x54f454d747e037da288db568d4121117eab34e79']
                }
            },
            'kyberswap': {
                'v1': {
                    'dynamic': '0x5f1fe642060b5b9658c15721ea22e982643c095c',
                    'static': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2': {
                    'elastic': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'dodo': {
                'v1': {
                    'dodo_birth': '0x357c5e9cfa8b834edcef7c7aabd8f9db09119d11',
                    'proxy': ['0xdbfaf391c37339c903503495395ad7d6b096e192',
                    '0x6c30be15d88462b788dea7c6a860a2ccaf7b2670']
                },
                'v2': {
                    'new_pool': ['0x95e887adf9eaa22cc1c6e3cb7f07adc95b4b25a8',
                    '0xd24153244066f0afa9415563bfc7ba248bfb7a51',
                    '0x43c49f8dd240e1545f147211ec9f917376ac1e87',
                    '0x79887f65f83bdf15bcc8736b5e5bcdb48fb8fe13']
                }
            },
            'fluid': {
                'v1': {
                    'factory': '0x91716c4eda1fb55e84bf8b4c7085f84285c19085'
                }
            },
            'hashflow': {
                'v1': {
                    'create': ['0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
                    '0x336bfba2c4d7bda5e1f83069d0a95509ecd5d2b5',
                    '0x9817a71ca8e309d654ee7e1999577bce6e6fd9ac']
                },
                'v3': {
                    'create_pool': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
                }
            },
            'woofi': {
                'v1': {
                    'woo_router_swap': ['0x9d1a92e601db0901e69bd810029f2c14bcca3128',
                    '0x817eb46d60762442da3d931ff51a30334ca39b74'],
                    'woo_swap': ['0x7081a38158bd050ae4a86e38e0225bc281887d7e',
                    '0x7400b665c8f4f3a951a99f1ee9872efb8778723d',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                },
                'v2': {
                    'woo_router_swap': '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7'
                }
            },
            'curve': {
                'v1': {
                    'deployer': ['0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0xbabe61887f1de2713c6f97e567623453d3c79f67',
                    '0x722272d36ef0da72ff51c5a65db7b870e2e8d4ee',
                    '0xe5de15a9c9bbedb4f5ec13b131e61245f2983a69']
                }
            },
            'balancer': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v2': {
                    'funds_deposited': ['0x9295ee1d8c5b022be115a2ad3c30c72e34e7f096','0x69b5c72837769ef1e7c164abc6515dcff217f920']
                },
                'v3': {
                    'funds_deposited': '0x9295ee1d8c5b022be115a2ad3c30c72e34e7f096'
                }
            },
            'allbridge': {
                'v1': {
                    'sent': '0xbbbd1bbb4f9b936c3604906d7592a644071de884'
                },
                'v2': {
                    'tokens_sent': '0x7775d63836987f444e2f14aa0fa2602204d7d3e0',
                    'lp': ['0x0394c4f17738a10096510832beab89a9dd090791',
                    '0x4c42dfdbb8ad654b42f66e0bd4dbdc71b52eb0a6',
                    '0x58cc621c62b0aa9babfae5651202a932279437da']
                }
            },
            'axelar': {
                'v1': {
                    'gateway': '0x6f015f16de9fc8791b234ef68d486d2bf203fba8',
                    'gas_service': '0x2d5d7d31f671f86c782533cc367f14109a082712',
                    'squid_router': '0xce16f69375520ab01377ce7b88f5ba8c48f8d666',
                    'burn': '0x0000000000000000000000000000000000000000'
                }
            },
            'chainlink_ccip': {
                'v1': {
                    'router': '0x849c5ed5a80f5b408dd4969b78c2c8fdf0565bfe'
                }
            },
            'circle_cctp': {
                'v1': {
                    'deposit': '0x9daf8c91aefae50b9c0e69629d3f6ca40ca3b3fe'
                },
                'v2': {
                    'deposit': '0x28b5a0e9c621a5badaa536219b3a228c8168cf5d'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': ['0x88dcdc47d2f83a99cf0000fdf667a468bb958a78',
                    '0xa251c4691c1ffd7d9b128874c023427513d8ac5c',
                    '0xb5df797468e6e8f2cb293cd6e32939366e0f8733',
                    '0x02745032d2aeccdc90310d6cca32cb82c7e149dd',
                    '0xf5c6825015280cdfd0b56903f9f8b5a2233476f5']
                }
            },
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
                }
            },
            'everclear': {
                'v1': {
                    'bridge': '0x7189c59e245135696bfd2906b56607755f84f3fd'
                }
            },
            'eywa': {
                'v1': {
                    'portal': ['0xece9cf6a8f2768a3b8b65060925b646afeaa5167',
                        '0xac8f44ceca92b2a4b30360e5bd3043850a0ffcbe',
                        '0xbf0b5d561b986809924f88099c4ff0e6bcce60c9'
                    ]
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
                    'bridge': '0x6ce9bf8cdab780416ad1fd87b318a077d2f50eac'
                }
            },
            'symbiosis': {
                'v1': {
                    'bridge': ['0xb8f275fbf7a959f4bce59999a2ef122a099e81a8',
                    '0x3338be49a5f60e2593337919f9ad7098e9a7dd7e']
                }
            },
            'synapse': {
                'v1': {
                    'token_bridge': ['0x8f5bbb2bb8c2ee94639e55d5f41de9b4839c1280',
                    '0x2119a5c9279a13ec0de5e30d572b316f1cfca567',
                    '0x0efc29e196da2e81afe96edd041bedcdf9e74893',
                    '0x5f06745ee8a2001198a379bafbd0361475f3cfc3',
                    '0x7103a324f423b8a4d4cc1c4f2d5b374af4f0bab5'],
                    'token_bridge_swap': ['0x8f5bbb2bb8c2ee94639e55d5f41de9b4839c1280',
                    '0x0efc29e196da2e81afe96edd041bedcdf9e74893']
                }
            },
            'wormhole': {
                'v1': {
                    'token_bridge': '0x5a58505a96d1dbf8df91cb21b54419fc36e93fde'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_ALLBRIDGE_SOURCE_CHAIN': 'polygon mainnet',
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '0x58c61aee5ed3d748a1467085ed2650b697a66234',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc',
        'CURATED_DEFI_LENDING_CONTRACT_MAPPING': {
            'compound': {
                'v3': {
                    'comp_v3_origin_from_address': ['0x6103db328d4864dc16bd2f0ee1b9a92e3f87f915', '0x2501713a67a3dedde090e42759088a7ef37d4eab']
                }
            },
            'aave': {
                'v3': {
                    'aave_treasury': '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c',
                    'aave_version_address': '0x794a61358d6845594f94dc1db02a252b5b4814ad',
                    'fork_version': 'v3'
                },
                'v2': {
                    'aave_treasury': '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c',
                    'aave_version_address': '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf',
                    'fork_version': 'v2'
                }
            },
            'morpho': {
                'v1': {
                    'morpho_blue_address': '0x1bf0c2541f820e775182832f06c0b7fc27a25f67'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 