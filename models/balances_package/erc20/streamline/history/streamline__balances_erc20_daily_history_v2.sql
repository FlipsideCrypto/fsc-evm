-- depends_on: {{ ref('streamline__balances_erc20_daily_records') }}
-- depends_on: {{ ref('streamline__balances_erc20_daily_complete') }}
-- depends_on: {{ ref('core__fact_blocks') }}

{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config(
    materialized = "table",
    tags = ['streamline', 'balances', 'history', 'erc20', 'phase_4']
) }}

{% if execute %}

    {% set block_dates_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.erc20_balances_daily_block_dates__intermediate_tmp AS
        SELECT DISTINCT block_date 
        FROM {{ ref("streamline__balances_erc20_daily_records") }}
    {% endset %}
    {% do run_query(block_dates_query) %}

    {% set snapshot_date_query %}
        SELECT MIN(block_date) AS snapshot_date 
        FROM silver.erc20_balances_daily_block_dates__intermediate_tmp
    {% endset %}

    {% set snapshot_date = run_query(snapshot_date_query)[0][0] %}
    {% if not snapshot_date or snapshot_date == 'None' %}
        {% set snapshot_date = '2099-01-01' %}
    {% endif %}

    {% set max_block_by_date_sql %}
        WITH base AS (
            SELECT
                block_timestamp :: DATE AS block_date,
                MAX(block_number) AS block_number
            FROM
                {{ ref("core__fact_blocks") }}
            WHERE block_timestamp >= dateadd('day',-1,'{{ vars.BALANCES_SL_START_DATE }}')
            GROUP BY
                block_timestamp :: DATE
        )
        SELECT
            block_date,
            block_number
        FROM
            base
        WHERE
            block_date <> (
                SELECT
                    MAX(block_date)
                FROM
                    base
            )
    {% endset %}

    {% set block_numbers_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.erc20_balances_daily_block_numbers__intermediate_tmp AS
        SELECT 
            block_number,
            block_date
        FROM ({{ max_block_by_date_sql }})
        WHERE block_date IN (
            SELECT block_date 
            FROM silver.erc20_balances_daily_block_dates__intermediate_tmp
        )
    {% endset %}
    {% do run_query(block_numbers_query) %}

    {% set snapshot_rows_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.erc20_balances_snapshot_rows__intermediate_tmp AS
        SELECT 
            block_date,
            address,
            contract_address
        FROM {{ ref("streamline__balances_erc20_daily_records") }} 
        WHERE block_date = '{{ snapshot_date }}'
        EXCEPT
        SELECT
            block_date,
            address,
            contract_address
        FROM {{ ref("streamline__balances_erc20_daily_complete") }}
        WHERE block_date = '{{ snapshot_date }}'
        LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
    {% endset %}
    {% do run_query(snapshot_rows_query) %}

    {% set daily_backfill_rows_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.erc20_balances_daily_backfill_rows__intermediate_tmp AS
        SELECT 
            block_date,
            address,
            contract_address
        FROM {{ ref("streamline__balances_erc20_daily_records") }} 
        WHERE block_date > '{{ snapshot_date }}'
        EXCEPT
        SELECT
            block_date,
            address,
            contract_address
        FROM {{ ref("streamline__balances_erc20_daily_complete") }}
        WHERE block_date > '{{ snapshot_date }}'
        LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
    {% endset %}
    {% do run_query(daily_backfill_rows_query) %}

{% endif %}

WITH to_do AS (
    SELECT
        block_date,
        address,
        contract_address
    FROM silver.erc20_balances_snapshot_rows__intermediate_tmp
    
    UNION ALL
    
    SELECT
        block_date,
        address,
        contract_address
    FROM silver.erc20_balances_daily_backfill_rows__intermediate_tmp
),
ranked_data as (

    SELECT
        block_number,
        DATE_PART('EPOCH_SECONDS', block_date)::INT AS block_date_unix,
        address,
        contract_address,
        DATE_PART('EPOCH_SECONDS', SYSDATE()::DATE)::INT AS partition_key,
        OBJECT_CONSTRUCT(
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
        ) as request,
        ROW_NUMBER() OVER (ORDER BY block_number DESC) as rn
    FROM to_do
    JOIN silver.erc20_balances_daily_block_numbers__intermediate_tmp USING (block_date)
)
SELECT
    block_number,
    block_date_unix,
    address,
    contract_address,
    partition_key,
    request
FROM ranked_data
WHERE rn <= {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
ORDER BY block_number DESC