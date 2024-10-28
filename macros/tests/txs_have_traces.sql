{% test txs_have_traces(model, transactions_model) %}

select 
    block_number,
    tx_hash,
    tx_position
from {{ transactions_model }} txs
left join {{ model }} tr using (
    block_number,
    tx_hash,
    tx_position
)
where tr.tx_hash is null or tr.tx_position is null or tr.block_number is null

{% endtest %}