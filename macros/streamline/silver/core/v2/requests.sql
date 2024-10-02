
{% macro streamline_core_requests() %}

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

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set params = {
    "external_table": var((trimmed_model ~ '_' ~ model_type ~ '_external_table').upper(), trimmed_model),
    "sql_limit": var((trimmed_model ~ '_' ~ model_type ~ '_sql_limit').upper(), 2 * var('BLOCKS_PER_HOUR')),
    "producer_batch_size": var((trimmed_model ~ '_' ~ model_type ~ '_producer_batch_size').upper(), 2 * var('BLOCKS_PER_HOUR')),
    "worker_batch_size": var(
        (trimmed_model ~ '_' ~ model_type ~ '_worker_batch_size').upper(), 
        (2 * var('BLOCKS_PER_HOUR')) // model_configs.get(trimmed_model, {}).get('lambdas', 1)
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

{# Set uses_receipts_by_hash based on model configuration #}
{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{#

-- match key / value to the vars
-- update order to match below

1. name output
2. variable section
3. rpc details
4. api details
5. dbt config

...model...
- only log in compile mode or runs in dev
#}

{% set config_dict = {
    'materialized': config.get('materialized'),
    'schema': config.get('schema'),
    'database': config.get('database'),
    'alias': config.get('alias'),
    'tags': config.get('tags'),
    'post_hook': config.get('post_hook'),
    'pre_hook': config.get('pre_hook'),
    'unique_key': config.get('unique_key'),
    'strategy': config.get('strategy'),
    'full_refresh': config.get('full_refresh'),
    'enabled': config.get('enabled'),
    'persist_docs': config.get('persist_docs'),
    'quoting': config.get('quoting'),
    'on_schema_change': config.get('on_schema_change'),
    'meta': config.get('meta'),
    'grants': config.get('grants'),
    'packages': config.get('packages'),
    'docs': config.get('docs')
} %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Name Output Details ===", info=True) }}
    {{ log("Original Model: " ~ model, info=True) }}
    {{ log("Trimmed Model: " ~ trimmed_model, info=True) }}
    {{ log("Trim Suffix: " ~ trim_suffix, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("model_quantum_state: " ~ model_quantum_state, info=True) }}
    {{ log("sql_limit: " ~ sql_limit, info=True) }}
    {{ log("testing_limit: " ~ testing_limit, info=True) }}
    {{ log("order_by_clause: " ~ order_by_clause, info=True) }}
    {{ log("new_build: " ~ new_build, info=True) }}
    {% if uses_receipts_by_hash and trimmed_model.lower().startswith('receipts') %}
        {{ log("uses_receipts_by_hash: " ~ uses_receipts_by_hash, info=True) }}
    {% endif %}

    {{ log("=== RPC Details ===", info=True) }}
    {{ log(trimmed_model ~ ": {", info=True) }}
    {{ log("    method: '" ~ model_configs[trimmed_model]['method'] ~ "',", info=True) }}
    {{ log("    params: '" ~ model_configs[trimmed_model]['params'] ~ "'", info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {{ log("=== API Details ===", info=True) }}
    {{ log("API_URL: " ~ var('API_URL'), info=True) }}
    {{ log("VAULT_SECRET_PATH: " ~ var('VAULT_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {{ log("Config Details:", info=True) }}
    {% for key, value in config_dict.items() %}
        {% if value is not none %}
            {% if key in ['pre_hook', 'post_hook'] %}
                {{ log(key ~ ": " ~ value | map(attribute='sql') | list | tojson, info=True) }}
            {% else %}
                {{ log(key ~ ": " ~ value | tojson, info=True) }}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Separately handle pre_hook and post_hook #}
    {% if config.get('pre_hook') %}
        {{ log("pre_hook: " ~ config.get('pre_hook') | map(attribute='sql') | list | tojson, info=True) }}
    {% endif %}

    {% if config.get('post_hook') %}
        {{ log("post_hook: " ~ config.get('post_hook') | map(attribute='sql') | list | tojson, info=True) }}
    {% endif %}
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

{# Special logic for receipts by hash #}

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
            'Vault/prod/core/ankr/mainnet' -- update to streamline var
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
         '{{ var('API_URL') }}',
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
        '{{ var('VAULT_SECRET_PATH') }}'
    ) AS request
FROM
    ready_blocks

{{ order_by_clause }}

LIMIT {{ sql_limit }}

{% endif %}

{% endmacro %}