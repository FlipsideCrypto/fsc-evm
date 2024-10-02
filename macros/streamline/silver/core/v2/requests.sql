
{% macro streamline_core_requests() %}

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

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set params = {
    "external_table": trimmed_model,
    "sql_limit": var((trimmed_model ~ '_' ~ model_type ~ '_sql_limit').upper()),
    "producer_batch_size": var((trimmed_model ~ '_' ~ model_type ~ '_producer_batch_size').upper()),
    "worker_batch_size": var((trimmed_model ~ '_' ~ model_type ~ '_worker_batch_size').upper()),
    "sql_source": model
} -%}

{# Set sql_limit variable for use in the main query #}
{%- set sql_limit = params['sql_limit'] -%}

{# Handle exploded key if it exists by updating the params dictionary above #}
{%- set exploded_key_var = (trimmed_model ~ '_exploded_key').upper() -%}
{%- set exploded_key_value = var(exploded_key_var, none) -%} 
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

{# Set uses_receipts_by_hash based on model configuration #}
{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{# Define model-specific RPC method and params #}
{%- set model_configs = {
    'blocks_transactions': {'method': 'eth_getBlockByNumber', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)'},
    'receipts': {'method': 'eth_getBlockReceipts', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))'},
    'receipts_by_hash': {'method': 'eth_getTransactionReceipt', 'params': 'ARRAY_CONSTRUCT(tx_hash)'},
    'traces': {'method': 'debug_traceBlockByNumber', 'params': "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))"},
    'confirmed_blocks': {'method': 'eth_getBlockByNumber', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'}
} -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("Original Model: " ~ model, info=True) }}
    {{ log("Trimmed Model: " ~ trimmed_model, info=True) }}
    {{ log("Trim Suffix: " ~ trim_suffix, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("Model Quantum State: " ~ model_quantum_state, info=True) }}
    {{ log("Query Limit: " ~ sql_limit, info=True) }}
    {{ log("Testing Limit: " ~ testing_limit, info=True) }}
    {{ log("Order By Clause: " ~ order_by_clause, info=True) }}
    {{ log("New Build: " ~ new_build, info=True) }}
    {{ log("Materialization: " ~ config.get('materialized'), info=True) }}
    {% if uses_receipts_by_hash and trimmed_model.lower().startswith('receipts') %}
        {{ log("Uses Receipts by Hash: " ~ uses_receipts_by_hash, info=True) }}
    {% endif %}
    {{ log("", info=True) }}

    {{ log("=== Streamline Parameters ===", info=True) }}
    {%- for key, value in params.items() %}
    {{ log(key ~ ": " ~ value, info=True) }}
    {%- endfor %}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}
    {{ log(trimmed_model ~ ": {", info=True) }}
    {{ log("    method: '" ~ model_configs[trimmed_model]['method'] ~ "',", info=True) }}
    {{ log("    params: '" ~ model_configs[trimmed_model]['params'] ~ "'", info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}
    {{ log("API URL: " ~ var('API_URL'), info=True) }}
    {{ log("Vault Secret Path: " ~ var('VAULT_SECRET_PATH'), info=True) }}
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

{% if not uses_receipts_by_hash or not trimmed_model.lower().startswith('receipts') %}

{# Main query starts here #}
WITH 
{% if not new_build %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),

    {% if trimmed_model == 'confirmed_blocks' %}
    {# For confirmed blocks, we want to exclude the last few hours of blocks to avoid any potential issues with reorgs #}
        look_back AS (
            SELECT block_number
            FROM {{ ref("_max_block_by_hour") }}
            QUALIFY ROW_NUMBER() OVER (ORDER BY block_number DESC) = 6
        ),
    {% endif %}
{% endif %}

{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
        {% if not new_build %}
        AND block_number
            {% if model_type == 'realtime' %}>={% elif model_type == 'history' %}<={% endif %}
            (SELECT block_number FROM last_3_days)
        {% endif %}
        {% if trimmed_model == 'confirmed_blocks' %}
            AND block_number <= (SELECT block_number FROM look_back)
        {% endif %}

    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM
        {% if trimmed_model == 'blocks_transactions' %}
            {{ ref("streamline__blocks_complete") }} b
            INNER JOIN {{ ref("streamline__transactions_complete") }} t USING(block_number)
        {% else %}
            {{ ref('streamline__' ~ trimmed_model ~ '_complete') }}
        {% endif %}
    WHERE 1=1
        {% if not new_build %}
            AND block_number
            {% if model_type == 'realtime' %}>={% elif model_type == 'history' %}<={% endif %}
            (SELECT block_number FROM last_3_days)
            {% if trimmed_model == 'confirmed_blocks' %}
                AND block_number IS NOT NULL
                AND block_number <= (SELECT block_number FROM look_back)
                AND _inserted_timestamp >= DATEADD('day', -4, SYSDATE())
                AND block_number >= (SELECT block_number FROM last_3_days)
            {% endif %}
        {% endif %}
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not new_build and trimmed_model != 'confirmed_blocks' %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}

        {% if trimmed_model in ['blocks_transactions', 'receipts'] %}
            UNION
            SELECT block_number
            FROM {{ ref("_missing_txs") }}
        {% endif %}

        {% if trimmed_model == 'receipts' %}
            UNION
            SELECT block_number
            FROM {{ ref("_missing_receipts") }}
        {% elif trimmed_model == 'traces' %}
            UNION
            SELECT block_number
            FROM {{ ref("_missing_traces") }}
        {% endif %}
    {% endif %}

    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }} 
    {% endif %}
)

{# Generate API requests for each block #}
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{{ var('API_URL') }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', '{{ model_configs[trimmed_model]['method'] }}',
            'params', {{ model_configs[trimmed_model]['params'] }}
        ),
        '{{ var('VAULT_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks
{{ order_by_clause }}

LIMIT {{ sql_limit }}

{% else %}

{{ config (
    materialized = "view",
    tags = ['streamline_core_realtime']
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
            '{Service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            'Vault/prod/core/ankr/mainnet'
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
    LIMIT
        10
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
         '{{ var('API_URL') }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        receipt_rpc_call,
        '{{ var('VAULT_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks

{% endif %}

{% endmacro %}