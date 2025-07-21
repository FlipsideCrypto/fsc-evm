{% macro mantle_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'mantle',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/mantle/quicknode/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WMNT',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'MNT',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': ['ETH', 'MNT'],
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': ['ethereum', 'mantle'],
        'MAIN_PRICES_PROVIDER_PLATFORMS': ['mantle', 'Mantle'],
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'MantleScan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.mantlescan.xyz/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/mantle_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '24,54 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *',
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'layerzero': {
                'v2': {
                    'bridge': '0x1a44076050125825900e736c501f859c50fe728c'
                }
            },
            'stargate': {
                'v2': {
                    'bridge': '0x41b491285a4f888f9f636cec8a363ab9770a0aef'
                }
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %}
