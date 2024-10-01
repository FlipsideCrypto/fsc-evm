
{% macro streamline_core_chainhead() %}

{%- set model_quantum_state = var('CHAINHEAD_QUANTUM_STATE', 'livequery') -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("Model Quantum State: " ~ model_quantum_state, info=True) }}
    {{ log("Materialization: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}
    {{ log("API URL: " ~ var('API_URL'), info=True) }}
    {{ log("Vault Secret Path: " ~ var('VAULT_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{{ config (
    materialized = 'table',
    tags = ['streamline_core_complete']
) }}

SELECT
    live.udf_api(
        'POST',
        '{{ var('API_URL') }}',
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
        '{{ var('VAULT_SECRET_PATH') }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
{% endmacro %}