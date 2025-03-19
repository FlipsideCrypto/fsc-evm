{% macro mantle_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'mantle',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/mantle/quicknode/mainnet',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8',
        'MAIN_SL_BLOCKS_PER_HOUR': 1800,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'MNT',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'mantle',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'MantleScan',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.mantlescan.xyz/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/mantle_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': '0x736a2ada5f4700d49da6b28a74c4a77cdb3e2994',
        'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': '0x5bcfc8ad38ee1da5f45d9795acadf57d37fec172',
        'CURATED_VERTEX_TOKEN_MAPPING': {
            'USDC': '0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9',
            'wMNT': '0x78c1b0c915c4faa5fffa6cabf0219da63d7f4cb8',
            'METH': '0xcda86a272531e8640cd7f1a92c01839911b90bb0',
            'WETH': '0xdeaddeaddeaddeaddeaddeaddeaddeaddead1111'
        }
    } %}
    
    {{ return(vars) }}
{% endmacro %}
