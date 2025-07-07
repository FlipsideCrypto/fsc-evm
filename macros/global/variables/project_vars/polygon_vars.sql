{% macro polygon_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'polygon',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/polygon/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WPOL',
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
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x6ce9bf8cdab780416ad1fd87b318a077d2f50eac',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c',
        'CURATED_DEX_POOLS_CONTRACT_MAPPING': {
            'uniswap': {
                'v2': {
                    'contract_address': '0x9e5a52f57b3038f1b8eee45f28b3c1967e22799c'
                },
                'v3': {
                    'contract_address': '0x1f98431c8ad98523631ae4a59f267346ea31f984'
                }
            },
            'sushiswap': {
                'v2': {
                    'contract_address': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'quickswap': {
                'v2': {
                    'contract_address': '0x5757371414417b8c6caad45baef941abc7d3ab32'
                },
                'v3': {
                    'contract_address': '0x411b0facc3489691f28ad58c47006af5e3ab3a28'
                }
            },
            'fraxswap': {
                'v2': {
                    'contract_address': ['0xc2544a32872a91f4a553b404c6950e89de901fdb',
                    '0x54f454d747e037da288db568d4121117eab34e79']
                }
            },
            'kyberswap': {
                'v1_dynamic': {
                    'contract_address': '0x5f1fe642060b5b9658c15721ea22e982643c095c'
                },
                'v1_static': {
                    'contract_address': '0x1c758af0688502e49140230f6b0ebd376d429be5'
                },
                'v2_elastic': {
                    'contract_address': '0x5f1dddbf348ac2fbe22a163e30f99f9ece3dd50a'
                }
            },
            'dodo': {
                'v1': {
                    'contract_address': '0x357c5e9cfa8b834edcef7c7aabd8f9db09119d11'
                },
                'v2': {
                    'contract_address': ['0x95e887adf9eaa22cc1c6e3cb7f07adc95b4b25a8',
                    '0xd24153244066f0afa9415563bfc7ba248bfb7a51',
                    '0x43c49f8dd240e1545f147211ec9f917376ac1e87',
                    '0x79887f65f83bdf15bcc8736b5e5bcdb48fb8fe13']
                }
            },
            'hashflow': {
                'v1': {
                    'contract_address': ['0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
                    '0x336bfba2c4d7bda5e1f83069d0a95509ecd5d2b5',
                    '0x9817a71ca8e309d654ee7e1999577bce6e6fd9ac']
                },
                'v3': {
                    'contract_address': '0xde828fdc3f497f16416d1bb645261c7c6a62dab5'
                }
            },
            'woofi': {
                'v1_router': {
                    'contract_address': ['0x9d1a92e601db0901e69bd810029f2c14bcca3128',
                    '0x817eb46d60762442da3d931ff51a30334ca39b74']
                },
                'v2_router': {
                    'contract_address': '0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7'
                },
                'v1': {
                    'contract_address': ['0x7081a38158bd050ae4a86e38e0225bc281887d7e',
                    '0x7400b665c8f4f3a951a99f1ee9872efb8778723d',
                    '0xed9e3f98bbed560e66b89aac922e29d4596a9642']
                }
            },
            'curve': {
                'v1': {
                    'contract_address': ['0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0xbabe61887f1de2713c6f97e567623453d3c79f67',
                    '0x722272d36ef0da72ff51c5a65db7b870e2e8d4ee',
                    '0xe5de15a9c9bbedb4f5ec13b131e61245f2983a69']
                }
            },
            'balancer': {
                'v1': {
                    'contract_address': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            }
        },
        'CURATED_DEX_DODO_PROXY_ADDRESSES': ['0xdbfaf391c37339c903503495395ad7d6b096e192',
        '0x6c30be15d88462b788dea7c6a860a2ccaf7b2670']
    } %}
    
    {{ return(vars) }}
{% endmacro %} 