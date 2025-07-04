{% macro arbitrum_vars() %}
    {% set vars = {
        'GLOBAL_PROJECT_NAME': 'arbitrum',
        'GLOBAL_NODE_PROVIDER': 'quicknode',
        'GLOBAL_NODE_VAULT_PATH': 'Vault/prod/evm/quicknode/arbitrum/mainnet',
        'GLOBAL_NODE_URL': '{URL}',
        'GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        'MAIN_SL_BLOCKS_PER_HOUR': 14200,
        'MAIN_PRICES_NATIVE_SYMBOLS': 'ETH',
        'MAIN_PRICES_NATIVE_BLOCKCHAINS': 'ethereum',
        'MAIN_PRICES_PROVIDER_PLATFORMS': 'Arbitrum',
        'DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME': 'arbiscan',
        'DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED': true,
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_URL': 'https://api.arbiscan.io/api?module=contract&action=getabi&address=',
        'DECODER_SL_CONTRACT_ABIS_EXPLORER_VAULT_PATH': 'Vault/prod/block_explorers/arbitrum_scan',
        'DECODER_SL_CONTRACT_ABIS_BRONZE_TABLE_ENABLED': true,
        'MAIN_GHA_STREAMLINE_CHAINHEAD_CRON': '5,35 * * * *',
        'MAIN_GHA_SCHEDULED_SCORES_CRON': '5 5 * * *',
        'MAIN_CORE_TRACES_ARB_MODE': true,
        'CURATED_VERTEX_OFFCHAIN_EXCHANGE_CONTRACT': '0xa4369d8e3dc847aedf17f4125f1abb1bc18fc060',
        'CURATED_VERTEX_CLEARINGHOUSE_CONTRACT': '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f',
        'CURATED_VERTEX_TOKEN_MAPPING': {
            'USDC': '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8',
            'WETH': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
            'wstETH': '0x5979d7b546e38e414f7e9822514be443a4800529',
            'WBTC': '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f',
            'ARB': '0x912ce59144191c1204e64559fe8253a0e49e6548',
            'USDT': '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
            'VRTX': '0x95146881b86b3ee99e63705ec87afe29fcc044d9',
            'TRUMPWIN': '0xe215d028551d1721c6b61675aec501b1224bd0a1',
            'HARRISWIN': '0xfbac82a384178ca5dd6df72965d0e65b1b8a028f'
        },
        'CURATED_BRIDGE_STARGATE_TOKEN_MESSAGING_CONTRACT': '0x19cfce47ed54a88614648dc3f19a5980097007dd',
        'CURATED_BRIDGE_LAYERZERO_ENDPOINT_V2_CONTRACT': '0x1a44076050125825900e736c501f859c50fe728c',
        'MAIN_OBSERV_EXCLUSION_LIST_ENABLED': true
    } %}
    
    {{ return(vars) }}
{% endmacro %} 