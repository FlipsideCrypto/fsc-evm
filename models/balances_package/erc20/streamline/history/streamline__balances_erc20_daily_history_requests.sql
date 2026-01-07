{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "table",
    tags = ['streamline','balances','history','erc20','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_date >= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
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
    --max daily block_number during the selected period, for each contract_address/address pair
    WHERE
        t.block_date IS NOT NULL
        AND d.block_date < (
            SELECT MAX(block_date)
            FROM last_x_days
        ) 
    EXCEPT
    SELECT
        block_number,
        block_date,
        address,
        contract_address
    FROM
        {{ ref("streamline__balances_erc20_daily_complete") }}
    WHERE block_date IS NOT NULL
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    address,
    contract_address,
    IFF(
        block_date = ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE,
        DATE_PART('EPOCH_SECONDS', SYSDATE() :: DATE) :: INT,
        ROUND(
            block_number,
            -3
        )
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
        'secret_name', 
        {% if vars.GLOBAL_ALT_NODE_ENABLED %}
        '{{ vars.GLOBAL_ALT_NODE_VAULT_PATH }}'
        {% else %}
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        {% endif %},
        'url', '{{ vars.GLOBAL_NODE_URL }}'
    ) AS request
FROM
    to_do
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
