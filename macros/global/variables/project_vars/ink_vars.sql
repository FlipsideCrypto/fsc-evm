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
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'across': {
                'v3': {
                    'funds_deposited': '0xef684c38f94f48775959ecf2012d7e864ffb9dd4'
                }
            },
            'everclear': {
                'v1': {
                    'bridge': '0x15a7ca97d1ed168fb34a4055cefa2e2f9bdb6c75'
                }
            },
            'layerzero': {
                'v2': {
                    'bridge': '0xa05a3380889115bf313f1db9d5f335157be4d816'
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