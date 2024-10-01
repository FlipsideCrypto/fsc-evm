
{% macro streamline_core_chainhead_v2() %}

{%- set model_quantum_state = var('CHAINHEAD_QUANTUM_STATE', 'livequery') -%}

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