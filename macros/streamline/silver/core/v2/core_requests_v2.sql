
{% macro streamline_core_requests_v2() %}

{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- set model = identifier_parts[1].split('_')[0] if '__' in this.identifier else this.identifier.split('_')[-2] -%}
{%- set model_type = identifier_parts[1].split('_')[1] if '__' in this.identifier else this.identifier.split('_')[-1] -%}
{%- set view_source = identifier_parts[1] if identifier_parts|length > 1 else this.identifier -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set params = {
    "external_table": model,
    "sql_limit": var((model ~ '_' ~ model_type ~ '_sql_limit').upper()),
    "producer_batch_size": var((model ~ '_' ~ model_type ~ '_producer_batch_size').upper()),
    "worker_batch_size": var((model ~ '_' ~ model_type ~ '_worker_batch_size').upper()),
    "sql_source": view_source
} -%}

{# Handle exploded key if it exists by updating the params dictionary above #}
{%- set exploded_key_var = (model ~ '_exploded_key').upper() -%}
{%- set exploded_key_value = var(exploded_key_var, none) -%} 
{%- if exploded_key_value is not none -%}
    {%- do params.update({"exploded_key": tojson(exploded_key_value)}) -%}
{%- endif -%}

{# Set additional configuration variables #}
{%- set model_quantum_state = var((model ~ '_quantum_state').upper(), 'streamline') -%}
{%- set query_limit = var((model ~ '_query_limit').upper(), none) -%}
{%- set testing_limit = var((model ~ '_testing_limit').upper(), none) -%}
{%- set order_by_clause = var('ORDER_BY_CLAUSE', 'ORDER BY partition_key ASC') -%}
{%- set new_build = var('NEW_BUILD', false) -%}

{# Define model-specific RPC method and params #}
{%- set model_configs = {
    'blocks_transactions': {'method': 'eth_getBlockByNumber', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)'},
    'receipts': {'method': 'eth_getBlockReceipts', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))'},
    'traces': {'method': 'debug_traceBlockByNumber', 'params': "ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))"},
    'confirmed_blocks': {'method': 'eth_getBlockByNumber', 'params': 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)'}
} -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("Model: " ~ model, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("Model Quantum State: " ~ model_quantum_state, info=True) }}
    {{ log("Query Limit: " ~ query_limit, info=True) }}
    {{ log("Testing Limit: " ~ testing_limit, info=True) }}
    {{ log("Order By Clause: " ~ order_by_clause, info=True) }}
    {{ log("New Build: " ~ new_build, info=True) }}
    {{ log("Materialization: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Streamline Parameters ===", info=True) }}
    {%- for key, value in params.items() %}
    {{ log(key ~ ": " ~ value, info=True) }}
    {%- endfor %}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}
    {{ log(model ~ ": {", info=True) }}
    {{ log("    method: '" ~ model_configs[model]['method'] ~ "',", info=True) }}
    {{ log("    params: '" ~ model_configs[model]['params'] ~ "'", info=True) }}
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

{# Main query starts here #}
WITH 
{% if not new_build %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),

    {% if model == 'confirmed_blocks' %}
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
        {% if model == 'confirmed_blocks' %}
            AND block_number <= (SELECT block_number FROM look_back)
        {% endif %}

    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM
        {% if model == 'blocks_transactions' %}
            {{ ref("streamline__complete_blocks") }} b
            INNER JOIN {{ ref("streamline__complete_transactions") }} t USING(block_number)
        {% else %}
            {{ ref('streamline__complete_' ~ model) }}
        {% endif %}
    WHERE 1=1
        {% if not new_build %}
            AND block_number
            {% if model_type == 'realtime' %}>={% elif model_type == 'history' %}<={% endif %}
            (SELECT block_number FROM last_3_days)
            {% if model == 'confirmed_blocks' %}
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

    {% if not new_build and model != 'confirmed_blocks' %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}

        {% if model in ['blocks_transactions', 'receipts'] %}
            UNION
            SELECT block_number
            FROM {{ ref("_missing_txs") }}
        {% endif %}

        {% if model == 'receipts' %}
            UNION
            SELECT block_number
            FROM {{ ref("_missing_receipts") }}
        {% elif model == 'traces' %}
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
            'method', {{ model_configs[model]['method'] }},
            'params', {{ model_configs[model]['params'] }}
        ),
        '{{ var('VAULT_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks
{{ order_by_clause }}
{% if query_limit is not none %}
    LIMIT {{ query_limit }} 
{% endif %}

{% endmacro %}