{# Set variables #}
{%- set model_name = 'READS' -%}
{%- set model_type = 'REALTIME' -%}

{%- set default_vars = set_default_variables_streamline(model_name, model_type) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set streamline_params = set_streamline_parameters_reads(
    model_name=model_name,
    model_type=model_type
) -%}

{%- set node_url = default_vars['node_url'] -%}
{%- set node_secret_path = default_vars['node_secret_path'] -%}
{%- set model_quantum_state = default_vars['model_quantum_state'] -%}
{%- set sql_limit = streamline_params['sql_limit'] -%}
{%- set testing_limit = default_vars['testing_limit'] -%}
{%- set order_by_clause = default_vars['order_by_clause'] -%}
{%- set method_params = streamline_params['method_params'] -%}
{%- set method = streamline_params['method'] -%}

{# Log configuration details #}
{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    node_url=node_url,
    model_quantum_state=model_quantum_state,
    sql_limit=sql_limit,
    testing_limit=testing_limit,
    order_by_clause=order_by_clause,
    streamline_params=streamline_params,
    method_params=method_params,
    method=method
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = streamline_params
    ),
    tags = ['streamline_reads_' ~ model_type.lower()]
) }}

{# Main query starts here #}
WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
to_do AS (
    SELECT
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM
        {{ ref("streamline__contract_reads") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM
        {{ ref("streamline__reads_complete") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
),
ready_calls AS (
    SELECT 
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM to_do

    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }}
    {% endif %}
)
SELECT
    contract_address,
    function_signature,
    call_name,
    function_input,
    block_number,
    CASE
        WHEN function_input IS NULL THEN function_signature
        WHEN function_input ILIKE '0x%' THEN CONCAT(
            function_signature,
            LPAD(SUBSTR(function_input, 3), 64, 0)
        )
        ELSE CONCAT(
            function_signature,
            LPAD(
                function_input,
                64,
                0
            )
        )
    END AS DATA,
    CONCAT((SYSDATE() :: DATE) :: STRING, '_', function_signature) AS partition_key,
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', '{{ method }}',
            'params', {{ method_params }}
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_calls

{{ order_by_clause }}

LIMIT {{ sql_limit }}