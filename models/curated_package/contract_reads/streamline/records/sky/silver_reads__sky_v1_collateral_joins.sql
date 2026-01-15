{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'sky_v1_collateral_joins_id',
    tags = ['silver','contract_reads','sky']
) }}

-- Sky Protocol (MakerDAO) collateral join discovery
-- Discovers join contracts from MCD_VAT rely events via gem() calls

WITH rely_events AS (
    SELECT
        DISTINCT
        LOWER(CONCAT('0x', SUBSTR(topic_1, -40))) AS join_address,
        block_number
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
        AND topic_0 = '0x65fae35e00000000000000000000000000000000000000000000000000000000'
        AND block_number >= 8928152 -- Contract deployment block
        {% if is_incremental() %}
        AND modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
        {% endif %}
)

SELECT
    join_address,
    OBJECT_CONSTRUCT(
        'id', CONCAT(join_address, '-gem'),
        'jsonrpc', '2.0',
        'method', 'eth_call',
        'params', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT(
                'to', join_address,
                'data', '0x7bd2bea7'
            ),
            utils.udf_int_to_hex(block_number)
        )
    ) AS rpc_request,
    live.udf_api(
        'POST',
        '{{ vars.GLOBAL_NODE_URL }}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'livequery'
        ),
        rpc_request,
        '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
    ) AS response,
    response:data:result::STRING AS result_hex,
    {{ dbt_utils.generate_surrogate_key(['join_address']) }} AS sky_v1_collateral_joins_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    rely_events