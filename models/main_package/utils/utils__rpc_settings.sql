{{ config(
    materialized = 'table',
    tags = ['rpc_settings']
) }}

select 
    blockchain,
    receipts_by_block,
    ARRAY_CONTAINS('mixHash'::VARIANT, blocks_fields) as use_mix_hash,
    ARRAY_CONTAINS('l1GasPrice'::VARIANT, receipts_fields) as use_l1_fees
from MANTLE_DEV.SILVER.BLOCKCHAIN_COMPATIBILITY_LOGS
where blockchain = 'swell' -- global prod db name 
qualify row_number() over (partition by blockchain order by inserted_at desc) = 1
