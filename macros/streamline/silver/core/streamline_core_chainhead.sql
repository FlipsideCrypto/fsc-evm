{% macro streamline_core_chainhead(
        vault_secret_path
    ) %}
SELECT
    live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'livequery'
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
        '{{ vault_secret_path }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
{% endmacro %}
