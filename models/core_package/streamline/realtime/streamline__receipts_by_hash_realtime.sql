{# Set uses_receipts_by_hash based on model configuration #}
{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}

{% set model_name = 'RECEIPTS_BY_HASH' %}
{% set model_type = 'REALTIME' %}

{%- set multiplier = var('AVG_TXS_PER_BLOCK', 1) -%}

{%- set streamline_params = set_streamline_parameters(
    model_name=model_name,
    model_type=model_type,
    lambdas=2,
    multiplier=multiplier
) -%}

{# Set sql_limit variable for use in the main query #}
{%- set sql_limit = streamline_params['sql_limit'] -%}

{# Set additional configuration variables #}
{%- set model_quantum_state = var((model_name ~ '_' ~ model_type ~ '_quantum_state').upper(), 'streamline') -%}
{%- set testing_limit = var((model_name ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}
{%- set new_build = var((model_name ~ '_' ~ model_type ~ '_new_build').upper(), false) -%}

{# Set order_by_clause based on model_type #}
{%- set default_order = 'ORDER BY partition_key DESC, block_number DESC' if model_type.lower() == 'realtime' else 'ORDER BY partition_key ASC, block_number ASC' -%}
{%- set order_by_clause = var((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper(), default_order) -%}

{%- set node_url = var('NODE_URL', '{Service}/{Authentication}') -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== API Details ===", info=True) }}

    {{ log("NODE_URL: " ~ node_url, info=True) }}
    {{ log("NODE_SECRET_PATH: " ~ var('NODE_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log((model_name ~ '_' ~ model_type ~ '_model_quantum_state').upper() ~ ': ' ~ model_quantum_state, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_sql_limit').upper() ~ ': ' ~ sql_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_testing_limit').upper() ~ ': ' ~ testing_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper() ~ ': ' ~ order_by_clause, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_new_build').upper() ~ ': ' ~ new_build, info=True) }}
    {{ log("USES_RECEIPTS_BY_HASH: " ~ uses_receipts_by_hash, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}

    {{ log(model_name ~ ": {", info=True) }}
    {{ log("    method: '" ~ 'eth_getTransactionReceipt' ~ "',", info=True) }}
    {{ log("    params: '" ~ 'ARRAY_CONSTRUCT(tx_hash)' ~ "'", info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {% set params_str = streamline_params | tojson %}
    {% set params_formatted = params_str | replace('{', '{\n            ') | replace('}', '\n        }') | replace(', ', ',\n            ') %}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    post_hook = fsc_utils.if_data_call_function_v2(\n' %}
    {% set config_log = config_log ~ '        func = "streamline.udf_bulk_rest_api_v2",\n' %}
    {% set config_log = config_log ~ '        target = "' ~ this.schema ~ '.' ~ this.identifier ~ '",\n' %}
    {% set config_log = config_log ~ '        params = ' ~ params_formatted ~ '\n' %}
    {% set config_log = config_log ~ '    ),\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = streamline_params
    ),
    tags = ['streamline_core_' ~ model_type.lower()]
) }}

{# Start by invoking LQ for the last hour of blocks #}

WITH numbered_blocks AS (

    SELECT
        block_number_hex,
        block_number,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS row_num
    FROM
        (
            SELECT
                *
            FROM
                {{ ref('streamline__blocks') }}
            ORDER BY
                block_number DESC
            LIMIT
                {{ var('BLOCKS_PER_HOUR') }}
        )
), batched_blocks AS (
    SELECT
        block_number_hex,
        block_number,
        100 AS rows_per_batch,
        CEIL(
            row_num / rows_per_batch
        ) AS batch_number,
        MOD(
            row_num - 1,
            rows_per_batch
        ) + 1 AS row_within_batch
    FROM
        numbered_blocks
),
batched_calls AS (
    SELECT
        batch_number,
        ARRAY_AGG(
            utils.udf_json_rpc_call(
                'eth_getBlockByNumber',
                [block_number_hex, false]
            )
        ) AS batch_request
    FROM
        batched_blocks
    GROUP BY
        batch_number
),
rpc_requests AS (
    SELECT
        live.udf_api(
            'POST',
            '{{ node_url }}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            '{{ var('NODE_SECRET_PATH') }}'
        ) AS resp
    FROM
        batched_calls
),
blocks AS (
    SELECT
        utils.udf_hex_to_int(
            VALUE :result :number :: STRING
        ) :: INT AS block_number,
        VALUE :result :transactions AS tx_hashes
    FROM
        rpc_requests,
        LATERAL FLATTEN (
            input => resp :data
        )
),
flat_tx_hashes AS (
    SELECT
        block_number,
        VALUE :: STRING AS tx_hash
    FROM
        blocks,
        LATERAL FLATTEN (
            input => tx_hashes
        )
),
to_do AS (
    SELECT
        block_number,
        tx_hash
    FROM
        flat_tx_hashes

    EXCEPT

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('streamline__' ~ model_name.lower() ~ '_complete') }}
    WHERE 1=1
        {% if not new_build %}
            AND block_number >= (SELECT block_number FROM last_3_days)
        {% endif %}
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }} 
    {% endif %}
)
SELECT
    block_number,
    tx_hash,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getTransactionReceipt',
            'params', ARRAY_CONSTRUCT(tx_hash)
        ),
        '{{ var('NODE_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks

{{ order_by_clause }}

LIMIT {{ sql_limit }}

{% endif %}