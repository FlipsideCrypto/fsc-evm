{% set model_name = 'RECEIPTS_BY_HASH' %}
{% set model_type = 'REALTIME' %}

{%- set default_vars = set_default_variables(model_name, model_type) -%}

{% if default_vars['uses_receipts_by_hash'] %}

{%- set multiplier = var('GLOBAL_AVG_TXS_PER_BLOCK', 1) -%}

{%- set streamline_params = set_streamline_parameters(
    model_name=model_name,
    model_type=model_type,
    lambdas=2,
    multiplier=multiplier
) -%}

{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    uses_receipts_by_hash=default_vars['uses_receipts_by_hash'],
    node_url=default_vars['node_url'],
    model_quantum_state=default_vars['model_quantum_state'],
    sql_limit=streamline_params['sql_limit'],
    testing_limit=default_vars['testing_limit'],
    order_by_clause=default_vars['order_by_clause'],
    new_build=default_vars['new_build'],
    streamline_params=streamline_params
) }}

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
                {{ var('GLOBAL_BLOCKS_PER_HOUR') }}
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
            '{{ default_vars['node_url'] }}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            '{{ default_vars['node_secret_path'] }}'
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
        {% if not default_vars['new_build'] %}
            AND block_number >= (SELECT block_number FROM last_3_days)
        {% endif %}
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    {% if default_vars['testing_limit'] is not none %}
        LIMIT {{ default_vars['testing_limit'] }} 
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
        '{{ default_vars['node_url'] }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state', '{{ default_vars['model_quantum_state'] }}'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getTransactionReceipt',
            'params', ARRAY_CONSTRUCT(tx_hash)
        ),
        '{{ default_vars['node_secret_path'] }}'
    ) AS request
FROM
    ready_blocks

{{ default_vars['order_by_clause'] }}

LIMIT {{ streamline_params['sql_limit'] }}

{% endif %}