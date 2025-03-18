{# Set variables #}
{%- set package_name = 'READS' -%}
{%- set model_name = 'READS' -%}
{%- set model_type = 'HISTORY' -%}

{%- set default_vars = set_default_variables_streamline(package_name, model_name, model_type) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set streamline_params = set_streamline_parameters_reads(
    package_name=package_name,
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
{{ log_model_details(
    vars = default_vars,    
    params = streamline_params    
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = streamline_params
    ),
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
WITH to_do AS ({% for item in range(17) %}
    (

    SELECT
        contract_address, function_signature, call_name, function_input, block_number
    FROM
        {{ ref("streamline__contract_reads") }}
    WHERE
        block_number BETWEEN {{ item * 1000000 + 1 }}
        AND {{(item + 1) * 1000000 }}
    EXCEPT
    SELECT
        contract_address, function_signature, call_name, function_input, block_number
    FROM
        {{ ref("streamline__reads_complete") }}
    WHERE
        block_number BETWEEN {{ item * 1000000 + 1 }}
        AND {{(item + 1) * 1000000 }}
    ORDER BY
        block_number) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),
ready_calls AS (
    SELECT
        contract_address,
        function_signature,
        call_name,
        function_input,
        block_number
    FROM
        to_do

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