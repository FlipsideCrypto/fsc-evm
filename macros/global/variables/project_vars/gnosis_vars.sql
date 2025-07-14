{% macro gnosis_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'gnosis',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/gnosis/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'XDAI',
        'MAIN_SL_BLOCKS_PER_HOUR': 1200,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['XDAI'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['Gnosis','Gnosis Chain'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'gnosisscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.gnosisscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/gnosis_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '25,55 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '45 5 * * *',
        'CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING': {
            'curve': {
                'v1': {
                    'deployer': ['0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
                    '0xcbaf0a32f5a16b326f00607421857f68fc72e508',
                    '0xd25fcbb7b6021cf83122fcd65be88a045d5f961c',
                    '0xd19baeadc667cf2015e395f2b08668ef120f41f5']
                }
            },
            'balancer': {
                'v1': {
                    'vault': '0xba12222222228d8ba445958a75a0704d566bf2c8'
                }
            },
            'sushiswap': {
                'v1': {
                    'uni_v2_pair_created': '0xc35dadb65012ec5796536bd9864ed8773abc74c4'
                }
            },
            'honeyswap': {
                'v1': {
                    'uni_v2_pair_created': '0xa818b4f111ccac7aa31d0bcc0806d64f2e0737d7'
                }
            },
            'swapr': {
                'v1': {
                    'uni_v2_pair_created': '0x5d48c95adffd4b40c1aaadc4e08fc44117e02179'
                }
            }
        },
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'chainlink_ccip': {
                'v1': {
                    'router': '0x4aad6071085df840abd9baf1697d5d5992bdadce'
                }
            },
            'celer_cbridge': {
                'v1': {
                    'bridge': '0x3795c36e7d12a8c252a20c5a7b455f7c57b60283'
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
                'v2': {
                    'bridge': '0xaf368c91793cb22739386dfcbbb2f1a9e4bcbebf'
                }
            }
        }
        'CURATED_DEFI_BRIDGE_HOP_BRIDGE_CONTRACT': '0x6f03052743cd99ce1b29265e377e320cd24eb632',
        'CURATED_DEFI_BRIDGE_HOP_TOKEN_CONTRACT': '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
    } %}
    
    {{ return(vars) }}
{% endmacro %}