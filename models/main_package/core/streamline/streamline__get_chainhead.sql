{%- set model_quantum_state = var('MAIN_SL_CHAINHEAD_QUANTUM_STATE', 'livequery') -%}

{%- set node_url = var('GLOBAL_NODE_URL', '{Service}/{Authentication}') -%}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'table',
    tags = ['streamline_core_complete','chainhead']
) }}

SELECT
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        '{{ var('GLOBAL_NODE_VAULT_PATH','') }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number