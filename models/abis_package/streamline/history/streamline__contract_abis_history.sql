{# Set variables #}
{%- set model_name = 'CONTRACT_ABIS' -%}
{%- set model_type = 'HISTORY' -%}

{%- set default_vars = set_default_variables_streamline(model_name, model_type) -%}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}
{%- set streamline_params = set_streamline_parameters_abis(
    model_name=model_name,
    model_type=model_type
) -%}

{%- set node_secret_path = var('CONTRACT_ABIS_SECRET_PATH') -%}
{%- set model_quantum_state = default_vars['model_quantum_state'] -%}
{%- set sql_limit = streamline_params['sql_limit'] -%}
{%- set testing_limit = default_vars['testing_limit'] -%}
{%- set order_by_clause = default_vars['order_by_clause'] -%}

{# Log configuration details #}
{{ log_streamline_details(
    model_name=model_name,
    model_type=model_type,
    model_quantum_state=model_quantum_state,
    sql_limit=sql_limit,
    testing_limit=testing_limit,
    order_by_clause=order_by_clause,
    streamline_params=streamline_params,
) }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = streamline_params
    ),
    tags = ['streamline_abis_' ~ model_type.lower()]
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
        created_contract_address AS contract_address,
        block_number
    FROM
        {{ ref("silver__created_contracts") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        contract_address,
        block_number
    FROM
        {{ ref("streamline__contract_abis_complete") }}
    WHERE
        block_number < (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_number IS NOT NULL
),
ready_abis AS (
    SELECT 
        contract_address,
        block_number
    FROM 
        to_do

    {% if testing_limit is not none %}  
        LIMIT {{ testing_limit }} 
    {% endif %}
)

SELECT
    block_number,
    contract_address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'GET',
        'https://api.etherscan.io/api?module=contract&action=getabi&address=' || contract_address || '&apikey={key}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', '{{ model_quantum_state }}'
        ),
        NULL,
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_abis

{{ order_by_clause }}

LIMIT {{ sql_limit }}