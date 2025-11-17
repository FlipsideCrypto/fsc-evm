{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "table",
    tags = ['streamline','balances','realtime','erc20','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
        WHERE block_date >= DATEADD('day', {{ vars.BALANCES_SL_DAILY_REALTIME_LOOKBACK_DAYS }}, SYSDATE())
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.address,
        t.contract_address
    FROM
        {{ ref("streamline__balances_erc20_daily_records") }} t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
        --max daily block_number from 1 day ago, for each contract_address/address pair
    WHERE
        t.block_date IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        block_date,
        address,
        contract_address
    FROM
        {{ ref("streamline__balances_erc20_daily_complete") }}
    WHERE
        block_date >= (
            SELECT MIN(block_date)
            FROM last_x_days
        )
        AND block_date IS NOT NULL
        AND _inserted_timestamp :: DATE >= DATEADD(
            'day',
            -7,
            SYSDATE()
        )
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    address,
    contract_address,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    OBJECT_CONSTRUCT(
        'data', OBJECT_CONSTRUCT(
            'id', CONCAT(
                contract_address,
                '-',
                address,
                '-',
                block_number
            ),
            'jsonrpc', '2.0',
            'method', 'eth_call',
            'params', ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to', contract_address,
                    'data', CONCAT(
                        '0x70a08231000000000000000000000000',
                        SUBSTR(address, 3)
                    )
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

LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_REALTIME_SQL_LIMIT }}