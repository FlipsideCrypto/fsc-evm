{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','admin','rpc_settings','phase_2']
) }}

select 
    blockchain,
    receipts_by_block,
    blocks_per_hour,
    blocks_fields,
    transactions_fields,
    receipts_fields,
    traces_fields,
    inserted_at as rpc_sampled_at
from
    {{ source(
        "fsc_evm_admin",
        "rpc_node_logs"
    ) }}
where lower(blockchain) = lower('{{ vars.GLOBAL_PROJECT_NAME }}')
and lower(network) = lower('{{ vars.GLOBAL_NETWORK }}')
qualify row_number() over (partition by blockchain, network order by rpc_sampled_at desc) = 1