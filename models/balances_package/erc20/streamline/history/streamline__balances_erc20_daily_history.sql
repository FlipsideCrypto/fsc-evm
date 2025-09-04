{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    tags = ['streamline','balances','history','erc20','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_number >= {{ vars.BALANCES_SL_START_BLOCK }}
),
verified_contracts AS (
    SELECT
        DISTINCT token_address
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),
logs AS (
    SELECT
        block_number,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS address2
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        (
            topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
            OR (
                topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
        )
        AND block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        AND block_number >= {{ vars.BALANCES_SL_START_BLOCK }} 
        --only include events prior to 1 day ago
        AND contract_address IN (
            SELECT
                token_address
            FROM
                verified_contracts
        )
),
transfers AS (
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        contract_address,
        address1 AS address
    FROM
        logs
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
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
        DISTINCT
        d.block_number,
        d.block_date,
        t.address,
        t.contract_address
    FROM
        transfers t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
    --max daily block_number during the selected period, for each contract_address/address pair
    WHERE
        t.block_date IS NOT NULL
        AND d.block_number < (
            SELECT MAX(block_number)
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
    WHERE
        block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        )
        AND block_number IS NOT NULL
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
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state',
            'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id',
            CONCAT(
                contract_address,
                '-',
                address,
                '-',
                block_number
            ),
            'jsonrpc',
            '2.0',
            'method',
            'eth_call',
            'params',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'to',
                    contract_address,
                    'data',
                    CONCAT(
                        '0x70a08231000000000000000000000000',
                        SUBSTR(
                            address,
                            3
                        )
                    )
                ),
                utils.udf_int_to_hex(block_number)
            )
        ),
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS request
FROM
    to_do
ORDER BY partition_key DESC, block_number DESC

LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}

{# Streamline Function Call #}
{% if execute %}
    {% set params = {
        "external_table": 'balances_erc20',
        "sql_limit": vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT,
        "producer_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
        "worker_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE,
        "async_concurrent_requests": vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS,
        "sql_source": 'balances_erc20_daily_history'
    } %}

    {% set function_call_sql %}
    {{ fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = this.schema ~ "." ~ this.identifier,
        params = params
    ) }}
    {% endset %}

    {% do run_query(function_call_sql) %}
    {{ log("Streamline function call: " ~ function_call_sql, info=true) }}
{% endif %}