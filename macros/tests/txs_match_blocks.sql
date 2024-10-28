{% test txs_match_blocks(model, blocks_model) %}

with count_txs as (
    select 
        block_number,
        count(*) as record_count
    from {{ model }}
    group by all
),

block_txs as (
    select
        block_number,
        tx_count as expected_count
    from {{ blocks_model }}
)
select
    block_number,
    record_count as actual_count,
    expected_count
from block_txs
left join count_txs using (block_number)
where record_count != expected_count or expected_count is null
{% endtest %}