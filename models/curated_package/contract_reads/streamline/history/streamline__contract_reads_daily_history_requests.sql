{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "table",
    tags = ['streamline','contract_reads','history','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_date >= ('{{ vars.CURATED_SL_CONTRACT_READS_START_DATE }}' :: TIMESTAMP) :: DATE
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.contract_address,
        t.address,
        t.function_name,
        t.function_sig,
        t.input,
        t.metadata,
        t.protocol,
        t.version,
        t.platform
    FROM
        {{ ref("streamline__contract_reads_daily_records") }} t
        CROSS JOIN last_x_days d
    EXCEPT
    SELECT
        block_number,
        block_date,
        contract_address,
        address,
        function_name,
        function_sig,
        input,
        metadata,
        protocol,
        version,
        platform
    FROM
        {{ ref("streamline__contract_reads_daily_complete") }}
    WHERE
        block_date IS NOT NULL
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    contract_address,
    address,
    function_name,
    function_sig,
    input,
    metadata :: STRING AS metadata_str,
    protocol,
    version,
    platform,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    OBJECT_CONSTRUCT(
        'data', OBJECT_CONSTRUCT(
            'id', CONCAT(
                contract_address,
                '-',
                COALESCE(address,'null'),
                '-',
                input,
                '-',
                block_number
            ),
            'jsonrpc', '2.0',
            'method', 'eth_call',
            'params', ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to', contract_address,
                    'data', input
                ),
                utils.udf_int_to_hex(block_number)
            )
        ),
        'headers', OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'x-fsc-livequery', 'true'
        ),
        'method', 'POST',
        'secret_name', '{{ vars.GLOBAL_NODE_VAULT_PATH }}',
        'url', '{{ vars.GLOBAL_NODE_URL }}'
    ) AS request
FROM
    to_do
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.CURATED_SL_CONTRACT_READS_DAILY_HISTORY_SQL_LIMIT }}