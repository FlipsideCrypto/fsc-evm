{# Set variables #}
{%- set model_name = 'ETH_BALANCES' -%}
{%- set model_type = 'HISTORY' -%}

{%- set default_vars = set_default_variables_streamline(model_name, model_type) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set streamline_params = set_streamline_parameters_balances(
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
    tags = ['streamline_balances_history']
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
traces AS (
    SELECT
        block_number,
        from_address,
        to_address
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number > 17000000
),
stacked AS (
    SELECT
        DISTINCT block_number,
        from_address AS address
    FROM
        traces
    WHERE
        from_address IS NOT NULL
        AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        to_address AS address
    FROM
        traces
    WHERE
        to_address IS NOT NULL
        AND to_address <> '0x0000000000000000000000000000000000000000'
),
to_do AS (
    SELECT
        block_number,
        address
    FROM
        stacked
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        address
    FROM
        {{ ref("streamline__eth_balances_complete") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number > 17000000
),
ready_blocks AS (
    SELECT
        block_number,
        address
    FROM
        to_do

    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }}
    {% endif %}
)
SELECT
    block_number,
    address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        '{{ node_url }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        OBJECT_CONSTRUCT(
            'id', CONCAT(address,'-',block_number),
            'jsonrpc', '2.0',
            'method', '{{ method }}',
            'params', {{ method_params }}
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks

{{ order_by_clause }}

LIMIT {{ sql_limit }}
