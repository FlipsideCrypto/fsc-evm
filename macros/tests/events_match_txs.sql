{% test events_match_txs(model, transactions_model) %}

with event_transactions as (
    select distinct
        block_number,
        tx_hash,
        tx_position
    from {{ model }}
),

missing_transactions as (
    select 
        event_transactions.block_number,
        event_transactions.tx_hash,
        event_transactions.tx_position
    from event_transactions
    left join {{ transactions_model }} transactions using (
        block_number,
        tx_hash,
        tx_position
    )
    where transactions.tx_hash is null
)

select * from missing_transactions

{% endtest %}