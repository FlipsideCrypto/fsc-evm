{% macro gnosis_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'gnosis',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/gnosis/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d',
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
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0xaf368c91793cb22739386dfcbbb2f1a9e4bcbebf',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c'
    } %}
    
    {{ return(vars) }}
{% endmacro %}