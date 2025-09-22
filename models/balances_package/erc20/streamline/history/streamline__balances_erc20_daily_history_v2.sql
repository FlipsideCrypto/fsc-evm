{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "table",
    tags = ['streamline','balances','history','erc20','phase_4']
) }}

{% if execute %}

{% set snapshot_date_query %}

select min(block_date) as snapshot_date from {{ ref("streamline__balances_erc20_daily_records") }}

{% endset %}

    {% set snapshot_date = run_query(snapshot_date_query) [0] [0] %}
    {% if not snapshot_date or snapshot_date == 'None' %}
        {% set snapshot_date = '2099-01-01' %}
    {% endif %}

{% set snapshot_rows_query %}
create or replace temporary table silver.erc20_balances_snapshot_rows__intermediate_tmp as
select 
    block_number,
    block_date,
    address,
    contract_address
from {{ ref("streamline__balances_erc20_daily_records") }} 
where block_date = '{{ snapshot_date }}'
except
select
    block_number,
    block_date,
    address,
    contract_address
from {{ ref("streamline__balances_erc20_daily_complete") }}
where block_date = '{{ snapshot_date }}'

limit {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}

{% endset %}
{% do run_query(snapshot_rows_query) %}

{% set daily_backfill_rows_query %}
create or replace temporary table silver.erc20_balances_daily_backfill_rows__intermediate_tmp as
select 
    block_number,
    block_date,
    address,
    contract_address
from {{ ref("streamline__balances_erc20_daily_records") }} 
where block_date > '{{ snapshot_date }}'
except
select
    block_number,
    block_date,
    address,
    contract_address
from {{ ref("streamline__balances_erc20_daily_complete") }}
where block_date > '{{ snapshot_date }}'

limit {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
{% endset %}
{% do run_query(daily_backfill_rows_query) %}

{% endif %}


WITH to_do AS (
    select
        block_number,
        block_date,
        address,
        contract_address
    from
       silver.erc20_balances_snapshot_rows__intermediate_tmp    
    union
    select
        block_number,
        block_date,
        address,
        contract_address
    from
        silver.erc20_balances_daily_backfill_rows__intermediate_tmp
    order by block_date desc
    LIMIT {{ vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT }}
)
SELECT
    block_number,
    DATE_PART('EPOCH_SECONDS', block_date) :: INT AS block_date_unix,
    address,
    contract_address,
    DATE_PART('EPOCH_SECONDS', SYSDATE()::DATE) :: INT AS partition_key,
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