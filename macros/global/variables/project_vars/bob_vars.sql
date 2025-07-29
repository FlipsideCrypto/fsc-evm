{% macro bob_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'bob',
        'GLOBAL_NODE_PROVIDER': 'drpc',
        'GLOBAL_NODE_URL': 'https://lb.drpc.org/ogrpc?network=bob&dkey={KEY}',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/drpc',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x4200000000000000000000000000000000000006',
        'GLOBAL_WRAPPED_NATIVE_ASSET_SYMBOL': 'WETH',
        'GLOBAL_NATIVE_ASSET_SYMBOL': 'ETH',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'bob-network',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'GoBOB',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://explorer-bob-mainnet-0.t.conduit.xyz/api/v2/smart-contracts/',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '21,51 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '20 5 * * *',
        'DECODER_SL_CONTRACT_ABIS_REALTIME_PRODUCER_BATCH_SIZE': 50,
        'DECODER_SL_CONTRACT_ABIS_REALTIME_WORKER_BATCH_SIZE': 50,
        'CURATED_DEFI_BRIDGE_CONTRACT_MAPPING': {
            'dln_debridge': {
                'v1': {
                    'source': '0xef4fb24ad0916217251f553c0596f8edc630eb66'
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
            }
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %} 