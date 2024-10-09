{% set model_name = 'TRACES' %}
{% set model_type = 'REALTIME' %}

{%- set streamline_params = set_streamline_parameters(
    model_name=model_name,
    model_type=model_type
) -%}

{%- set default_vars = set_default_variables(model_name, model_type) -%}

{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    node_url=default_vars['node_url'],
    model_quantum_state=default_vars['model_quantum_state'],
    sql_limit=streamline_params['sql_limit'],
    testing_limit=default_vars['testing_limit'],
    order_by_clause=default_vars['order_by_clause'],
    new_build=default_vars['new_build'],
    streamline_params=streamline_params,
    params=streamline_params['params'],
    method=streamline_params['method']
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

WITH 
{% if not default_vars['new_build'] %}
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_block_lookback") }}
    ),
{% endif %}

{# Identify blocks that need processing #}
to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__blocks") }}
    WHERE 
        block_number IS NOT NULL
    {% if not default_vars['new_build'] %}
        AND block_number >= (SELECT block_number FROM last_3_days)
    {% endif %}

    EXCEPT

    {# Exclude blocks that have already been processed #}
    SELECT block_number
    FROM {{ ref('streamline__' ~ model_name.lower() ~ '_complete') }}
    WHERE 1=1
    {% if not default_vars['new_build'] %}
        AND block_number >= (SELECT block_number FROM last_3_days)
    {% endif %}
)

{# Prepare the final list of blocks to process #}
,ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not default_vars['new_build'] %}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_traces") }}
    {% endif %}

    {% if default_vars['testing_limit'] is not none %}
        LIMIT {{ default_vars['testing_limit'] }} 
    {% endif %}
)

{# Generate API requests for each block #}
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        '{{ default_vars['node_url'] }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ default_vars['model_quantum_state'] }}'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', '{{ streamline_params['method'] }}',
            'params', {{ streamline_params['params'] }}
        ),
        '{{ default_vars['node_secret_path'] }}'
    ) AS request
FROM
    ready_blocks
    
{{ default_vars['order_by_clause'] }}

LIMIT {{ streamline_params['sql_limit'] }}