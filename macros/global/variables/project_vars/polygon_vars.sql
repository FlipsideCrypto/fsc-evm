{% macro polygon_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'polygon',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/polygon/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
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
        },
    } %}
    
    {{ return(vars) }}
{% endmacro %} 