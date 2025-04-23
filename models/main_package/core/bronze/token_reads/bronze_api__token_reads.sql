{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = vars.GLOBAL_BRONZE_FR_ENABLED,
    tags = ['bronze','core','token_reads','phase_2']
) }}

WITH base AS (

    SELECT
        contract_address,
        latest_event_block AS latest_block
    FROM
        {{ ref('silver__relevant_contracts') }}
    WHERE
        total_event_count >= 25

{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
ORDER BY
    total_event_count DESC
LIMIT
    50
), function_sigs AS (
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
    UNION
    SELECT
        '0x06fdde03',
        'name'
    UNION
    SELECT
        '0x95d89b41',
        'symbol'
),
all_reads AS (
    SELECT
        *
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(latest_block)],
            concat_ws(
                '-',
                contract_address,
                input,
                latest_block
            )
        ) AS rpc_request
    FROM
        all_reads
),
node_call AS (
    SELECT
        *,
        live.udf_api(
            'POST',
            '{{ vars.GLOBAL_NODE_URL }}',
            OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'livequery'
            ),
            rpc_request,
           '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS response
    FROM
        ready_reads
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                ready_reads
            LIMIT
                1
        )
)
SELECT
    contract_address,
    latest_block AS block_number,
    LEFT(input, 10) AS function_sig,
    NULL AS function_input,
    response:data:result::string as read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp
FROM
    node_call