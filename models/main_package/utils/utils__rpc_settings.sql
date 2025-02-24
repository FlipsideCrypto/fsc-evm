{{ config(
    materialized = 'table',
    tags = ['rpc_settings']
) }}

WITH latest_compatibility AS (
    SELECT *
    FROM {{ source('silver', 'blockchain_compatibility_logs') }}
    where blockchain = '{{ var('GLOBAL_PROD_DB_NAME').lower() }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blockchain ORDER BY inserted_at DESC) = 1
)

SELECT 
    blockchain,
    -- Block settings
    receipts_by_block,
    blocks_fields,
    array_contains('baseFeePerGas'::VARIANT, blocks_fields) as blocks_has_base_fee,
    array_contains('totalDifficulty'::VARIANT, blocks_fields) as blocks_has_total_difficulty,
    array_contains('mixHash'::VARIANT, blocks_fields) as blocks_has_mix_hash,
    array_contains('blobGasUsed'::VARIANT, blocks_fields) as blocks_has_blob_gas_used,
    array_contains('parentBeaconBlockRoot'::VARIANT, blocks_fields) as blocks_has_parent_beacon_block_root,
    array_contains('withdrawals'::VARIANT, blocks_fields) as blocks_has_withdrawals,
    
    -- Transaction settings
    transaction_fields,
    array_contains('accessList'::VARIANT, transaction_fields) as tx_has_access_list,
    array_contains('maxFeePerGas'::VARIANT, transaction_fields) as tx_has_max_fee_per_gas,
    array_contains('maxPriorityFeePerGas'::VARIANT, transaction_fields) as tx_has_max_priority_fee_per_gas,
    array_contains('blobGasPrice'::VARIANT, transaction_fields) as tx_has_blob_gas_price,
    array_contains('sourceHash'::VARIANT, transaction_fields) as tx_has_source_hash,
    array_contains('mint'::VARIANT, transaction_fields) as tx_has_mint,
    array_contains('ethValue'::VARIANT, transaction_fields) as tx_has_eth_value,
    array_contains('yParity'::VARIANT, transaction_fields) as tx_has_y_parity,
    array_contains('l1Fee'::VARIANT, receipts_fields) as tx_has_l1_columns,
    array_contains('l1FeeScalar'::VARIANT, receipts_fields) as tx_has_l1_tx_fee_calc,
    array_contains('l1BlobBaseFee'::VARIANT, receipts_fields) as tx_has_blob_base_fee,
    array_contains('maxFeePerGas'::VARIANT, transaction_fields) as tx_has_eip_1559
FROM latest_compatibility