{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'table',
    tags = ['silver','admin','rpc_settings','phase_1']
) }}

SELECT
    blockchain,
    receipts_by_block,
    blocks_per_hour,
    blocks_fields,
    transactions_fields,
    receipts_fields,
    traces_fields,
    inserted_at AS rpc_sampled_at
FROM
    {{ source(
        "fsc_evm_admin",
        "rpc_node_logs"
    ) }}
WHERE
    RESULT :error :: STRING IS NULL
    AND LOWER(blockchain) = LOWER('{{ vars.GLOBAL_PROJECT_NAME }}')
    AND LOWER(network) = LOWER('{{ vars.GLOBAL_NETWORK }}') qualify ROW_NUMBER() over (
        PARTITION BY blockchain,
        network
        ORDER BY
            rpc_sampled_at DESC
    ) = 1
