{# Set uses_receipts_by_hash based on model configuration #}
{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}

{# Define model-specific RPC method and params #}

{%- set model_configs = {
    'blocks_transactions': {
        'method': 'eth_getBlockByNumber',
        'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)',
        'exploded_key': ['data', 'result.transactions']
    },
    'receipts': {
        'method': 'eth_getBlockReceipts',
        'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))',
        'exploded_key': ['result'],
        'lambdas': 2
    },
    'receipts_by_hash': {
        'method': 'eth_getTransactionReceipt',
        'params': 'ARRAY_CONSTRUCT(tx_hash)'
    },
    'traces': {
        'method': 'debug_traceBlockByNumber',
        'params': "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))",
        'exploded_key': ['result'],
        'lambdas': 2
    },
    'confirmed_blocks': {
        'method': 'eth_getBlockByNumber',
        'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'
    }
} -%}

{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- if '__' in this.identifier -%}
    {%- set model = identifier_parts[1] -%}
{%- else -%}
    {%- set model = this.identifier -%}
{%- endif -%}

{# Dynamically get the trim suffix for this specific model #}
{% set trim_suffix = var((model ~ '_trim_suffix').upper(), '_realtime') %}

{# Trim model name logic and extract model_type #}
{%- if trim_suffix and model.endswith(trim_suffix) -%}
    {%- set trimmed_model = model[:model.rfind(trim_suffix)] -%}
    {%- set model_type = trim_suffix[1:] -%}  {# Remove the leading underscore #}
{%- else -%}
    {%- set trimmed_model = model -%}
    {%- set model_type = '' -%}
{%- endif -%}

{%- set multiplier = var('AVG_TXS_PER_BLOCK', 1) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set params = {
    "external_table": var((trimmed_model ~ '_' ~ model_type ~ '_external_table').upper(), trimmed_model),
    "sql_limit": var((trimmed_model ~ '_' ~ model_type ~ '_sql_limit').upper(), 2 * var('BLOCKS_PER_HOUR') * multiplier),
    "producer_batch_size": var((trimmed_model ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 2 * var('BLOCKS_PER_HOUR') * multiplier),
    "worker_batch_size": var(
        (trimmed_model ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 
        (2 * var('BLOCKS_PER_HOUR') * multiplier) // model_configs.get(trimmed_model, {}).get('lambdas', 1)
    ),
    "sql_source": model
} -%}

{# Set sql_limit variable for use in the main query #}
{%- set sql_limit = params['sql_limit'] -%}

{# Handle exploded key if it exists by updating the params dictionary above #}
{%- set exploded_key_var = (trimmed_model ~ '_exploded_key').upper() -%}
{%- set exploded_key_value = var(exploded_key_var, model_configs.get(trimmed_model, {}).get('exploded_key')) -%}
{%- if exploded_key_value is not none -%}
    {%- do params.update({"exploded_key": tojson(exploded_key_value)}) -%}
{%- endif -%}

{# Set additional configuration variables #}
{%- set model_quantum_state = var((trimmed_model ~ '_' ~ model_type ~ '_quantum_state').upper(), 'streamline') -%}
{%- set testing_limit = var((trimmed_model ~ '_' ~ model_type ~ '_testing_limit').upper(), none) -%}
{%- set new_build = var((trimmed_model ~ '_' ~ model_type ~ '_new_build').upper(), false) -%}

{# Set order_by_clause based on model_type #}
{%- set default_order = 'ORDER BY partition_key DESC, block_number DESC' if model_type == 'realtime' else 'ORDER BY partition_key ASC, block_number ASC' -%}
{%- set order_by_clause = var((trimmed_model ~ '_' ~ model_type ~ '_order_by_clause').upper(), default_order) -%}

{%- set node_url = var('NODE_URL', '{Service}/{Authentication}') -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Name Output Details ===", info=True) }}

    {{ log("Original Model: " ~ model, info=True) }}
    {{ log("Trimmed Model: " ~ trimmed_model, info=True) }}
    {{ log("Trim Suffix: " ~ trim_suffix, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}

    {{ log("NODE_URL: " ~ node_url, info=True) }}
    {{ log("NODE_SECRET_PATH: " ~ var('NODE_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log((trimmed_model ~ '_' ~ model_type ~ '_model_quantum_state').upper() ~ ': ' ~ model_quantum_state, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_sql_limit').upper() ~ ': ' ~ sql_limit, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_testing_limit').upper() ~ ': ' ~ testing_limit, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_order_by_clause').upper() ~ ': ' ~ order_by_clause, info=True) }}
    {{ log((trimmed_model ~ '_' ~ model_type ~ '_new_build').upper() ~ ': ' ~ new_build, info=True) }}
    {{ log("USES_RECEIPTS_BY_HASH: " ~ uses_receipts_by_hash, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}

    {{ log(trimmed_model ~ ": {", info=True) }}
    {{ log("    method: '" ~ model_configs[trimmed_model]['method'] ~ "',", info=True) }}
    {{ log("    params: '" ~ model_configs[trimmed_model]['params'] ~ "'", info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {% set params_str = params | tojson %}
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
        params = params
    ),
    tags = ['streamline_core_' ~ model_type]
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
create_receipts_calls AS (
    SELECT
        block_number,
        VALUE :: STRING AS tx_hash,
        utils.udf_json_rpc_call(
            'eth_getTransactionReceipt',
            [tx_hash]
        ) AS receipt_rpc_call
    FROM
        blocks,
        LATERAL FLATTEN (
            input => tx_hashes
        )
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash,
        receipt_rpc_call
    FROM
        create_receipts_calls

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
            'method', '{{ model_configs[trimmed_model]['method'] }}',
            'params', {{ model_configs[trimmed_model]['params'] }}
        ),
        '{{ var('NODE_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks

{{ order_by_clause }}

LIMIT {{ sql_limit }}

{% endif %}