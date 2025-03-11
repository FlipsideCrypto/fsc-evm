{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['rpc_settings']
) }}

select 
    blockchain,
    receipts_by_block,
    blocks_per_hour,
    blocks_fields,
    transactions_fields,
    receipts_fields,
    traces_fields,
    inserted_at
from
    {{ source(
        "fsc_evm_admin",
        "blockchain_compatibility_logs"
    ) }}
where lower(blockchain) = lower('{{ vars.GLOBAL_PROJECT_NAME }}')
qualify row_number() over (partition by blockchain order by inserted_at desc) = 1