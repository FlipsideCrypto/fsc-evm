{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "table",
    tags = ['streamline','balances','history','native','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_date >= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
),
to_do_snapshot AS (
    SELECT
        DISTINCT
        block_date,
        address
    FROM
        {{ ref("streamline__balances_native_daily_records") }}
    WHERE
        block_date = ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    EXCEPT
    SELECT
        block_date,
        address
    FROM
        {{ ref("streamline__balances_native_daily_complete") }}
    WHERE block_date = ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    LIMIT {{ vars.BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT }}
),
to_do_daily AS (
    SELECT
        DISTINCT
        block_date,
        address
    FROM
        {{ ref("streamline__balances_native_daily_records") }}
    WHERE
        block_date > ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    EXCEPT
    SELECT
        block_date,
        address
    FROM
        {{ ref("streamline__balances_native_daily_complete") }}
    WHERE block_date > ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    ORDER BY block_date DESC
    LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
),
to_do AS (
    SELECT
        block_date,
        'snapshot' AS TYPE,
        address
    FROM to_do_snapshot
    UNION ALL
    SELECT
        block_date,
        'daily' AS TYPE,
        address
    FROM to_do_daily
),
to_do_ranked AS (
    SELECT
        block_number,
        block_date,
        DATE_PART('EPOCH_SECONDS', block_date)::INT AS block_date_unix,
        TYPE,
        address,
        IFF(
            TYPE = 'snapshot',
            DATE_PART('EPOCH_SECONDS', SYSDATE() :: DATE) :: INT,
            ROUND(
                block_number,
                -3
            )
        ) AS partition_key,
        ROW_NUMBER() OVER (ORDER BY block_number DESC) AS rn
    FROM
        to_do
        JOIN last_x_days USING (block_date)
),
to_do_batched AS (
SELECT
    block_number,
    block_date,
    block_date_unix,
    TYPE,
    address,
    partition_key,
    api_request,
    rn
FROM
    to_do_ranked
WHERE
    rn <= {{ vars.BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT }}
ORDER BY
    block_date DESC --included for performance reasons
)
SELECT
    block_number,
    block_date_unix,
    address,
    partition_key,
    OBJECT_CONSTRUCT(
        'data', OBJECT_CONSTRUCT(
            'id',
            CONCAT(
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBalance',
            'params',
            ARRAY_CONSTRUCT(address, utils.udf_int_to_hex(block_number))
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
    to_do_batched

LIMIT {{ vars.BALANCES_SL_NATIVE_DAILY_HISTORY_SQL_LIMIT }}
