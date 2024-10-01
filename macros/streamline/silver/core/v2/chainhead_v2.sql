{% macro streamline_core_chainhead_v2(
    quantum_state,
    vault_secret_path,
    api_url
) %}
SELECT
    live.udf_api(
        'POST',
        '{{ api_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        {% if quantum_state == 'streamline' %}
            ,'fsc-quantum-state',
            'streamline'
        ),
        {% elif quantum_state == 'livequery' %}
            ,'fsc-quantum-state',
            'livequery'
        ),
        {% else %}
        ),
        {% endif %}
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
        '{{ vault_secret_path }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
{% endmacro %}