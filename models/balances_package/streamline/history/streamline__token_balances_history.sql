{# Set variables #}
{%- set model_name = 'TOKEN_BALANCES' -%}
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
logs AS (
    SELECT
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 42)) AS address2,
        l.contract_address,
        l.block_number
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        (
            l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                l.topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND l.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            )
            OR (
                l.topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND l.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            )
        )
        AND block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number > 17000000
),
transfers AS (
    SELECT
        DISTINCT block_number,
        contract_address,
        address1 AS address
    FROM
        logs
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        contract_address,
        address2 AS address
    FROM
        logs
    WHERE
        address2 IS NOT NULL
        AND address2 <> '0x0000000000000000000000000000000000000000'
),
to_do AS (
    SELECT
        block_number,
        address,
        contract_address
    FROM
        transfers
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        address,
        contract_address
    FROM
        {{ ref("streamline__token_balances_complete") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
        AND block_number > 17000000
),
ready_blocks AS (
    SELECT
        block_number,
        address,
        contract_address
    FROM
        to_do

    {% if testing_limit is not none %}
        LIMIT {{ testing_limit }}
    {% endif %}
)
SELECT
    block_number,
    address,
    contract_address,
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
            'id', CONCAT(contract_address,'-',address,'-',block_number),
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
