{% set model_name = 'BLOCKS_TRANSACTIONS' %}
{% set model_type = 'REALTIME' %}
{% set exploded_key = ['data', 'result.transactions'] %}

{# Set up parameters for the streamline process. These will come from the vars set in dbt_project.yml #}

{%- set streamline_params = set_streamline_parameters(
    model_name=model_name,
    model_type=model_type,
    exploded_key=exploded_key
) -%}

{# Set sql_limit variable for use in the main query #}
{%- set sql_limit = streamline_params['sql_limit'] -%}

{%- set default_vars = set_default_variables(model_name, model_type) -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== API Details ===", info=True) }}

    {{ log("NODE_URL: " ~ node_url, info=True) }}
    {{ log("NODE_SECRET_PATH: " ~ var('NODE_SECRET_PATH'), info=True) }}
    {{ log("", info=True) }}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log((model_name ~ '_' ~ model_type ~ '_model_quantum_state').upper() ~ ': ' ~ model_quantum_state, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_sql_limit').upper() ~ ': ' ~ sql_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_testing_limit').upper() ~ ': ' ~ testing_limit, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_order_by_clause').upper() ~ ': ' ~ order_by_clause, info=True) }}
    {{ log((model_name ~ '_' ~ model_type ~ '_new_build').upper() ~ ': ' ~ new_build, info=True) }}
    {{ log("", info=True) }}

    {{ log("=== RPC Details ===", info=True) }}

    {{ log(model_name ~ ": {", info=True) }}
    {{ log("    method: '" ~ 'eth_getBlockByNumber' ~ "',", info=True) }}
    {{ log("    params: '" ~ 'ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)' ~ "'", info=True) }}
    {{ log("}", info=True) }}
    {{ log("", info=True) }}

    {% set params_str = streamline_params | tojson %}
    {% set params_formatted = params_str | replace('{', '{\n            ') | replace('}', '\n        }') | replace(', ', ',\n            ') %}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    post_hook = fsc_utils.if_data_call_function_v2(\n' %}
    {% set config_log = config_log ~ '        func = "streamline.udf_bulk_rest_api_v2",\n' %}
    {% set config_log = config_log ~ '        target = "' ~ this.schema ~ '.' ~ this.identifier ~ '",\n' %}
    {% set config_log = config_log ~ '        params = ' ~ params_formatted ~ '\n' %}
    {% set config_log = config_log ~ '    ),\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

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

    SELECT block_number
    FROM {{ ref("streamline__blocks_complete") }} b
    INNER JOIN {{ ref("streamline__transactions_complete") }} t USING(block_number)
    WHERE 1=1
    {% if not default_vars['new_build'] %}
        AND block_number >= (SELECT block_number FROM last_3_days)
    {% endif %}
),
ready_blocks AS (
    SELECT block_number
    FROM to_do

    {% if not default_vars['new_build']%}
        UNION
        SELECT block_number
        FROM {{ ref("_unconfirmed_blocks") }}
        UNION
        SELECT block_number
        FROM {{ ref("_missing_txs") }}
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
            'method', 'eth_getBlockByNumber',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)
        ),
        '{{ default_vars['node_secret_path'] }}'
    ) AS request
FROM
    ready_blocks
    
{{ default_vars['order_by_clause'] }}

LIMIT {{ sql_limit }}
