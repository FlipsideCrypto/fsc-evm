{{ config(
    materialized = 'table',
    tags = ['rpc_settings']
) }}

select 
    blockchain,
    receipts_by_block,
    blocks_fields,
    array_contains('baseFeePerGas'::VARIANT, blocks_fields) as blocks_has_base_fee,
    array_contains('totalDifficulty'::VARIANT, blocks_fields) as blocks_has_total_difficulty,
    array_contains('mixHash'::VARIANT, blocks_fields) as blocks_has_mix_hash,
    array_contains('blobGasUsed'::VARIANT, blocks_fields) as blocks_has_blob_gas_used,
    array_contains('parentBeaconBlockRoot'::VARIANT, blocks_fields) as blocks_has_parent_beacon_block_root,
    array_contains('withdrawals'::VARIANT, blocks_fields) as blocks_has_withdrawals
from MANTLE_DEV.SILVER.BLOCKCHAIN_COMPATIBILITY_LOGS
where blockchain = 'swell' -- global prod db name 
qualify row_number() over (partition by blockchain order by inserted_at desc) = 1
