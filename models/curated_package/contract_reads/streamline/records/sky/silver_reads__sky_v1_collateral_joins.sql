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
-- Discovers join contracts from MCD_VAT rely events and maps to underlying tokens via gem() calls

WITH rely_events AS (
    SELECT DISTINCT
        LOWER(CONCAT('0x', SUBSTR(topic_1, -40))) AS join_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b'
        AND topic_0 = '0x65fae35e00000000000000000000000000000000000000000000000000000000'
        AND block_number >= 8928152 -- Contract deployment block
        {% if is_incremental() %}
        AND modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
        {% endif %}
),

gem_calls AS (
    SELECT
        join_address,
        live.udf_api(
            'POST',
            '{URL}',
            OBJECT_CONSTRUCT(
                'Content-Type', 'application/json',
                'fsc-quantum-state', 'livequery'
            ),
            utils.udf_json_rpc_call(
                'eth_call',
                [
                    {
                        'to': join_address,
                        'from': null,
                        'data': '0x7bd2bea7'
                    },
                    'latest'
                ],
                concat_ws('-', join_address, 'gem')
            ),
            '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS api_response
    FROM
        rely_events
),

parsed_joins AS (
    SELECT
        join_address,
        api_response:data:result::STRING AS result_hex,
        CASE
            WHEN result_hex IS NOT NULL
            AND LENGTH(result_hex) >= 42
            AND result_hex != '0x'
            AND result_hex != '0x0000000000000000000000000000000000000000000000000000000000000000'
            THEN LOWER(CONCAT('0x', SUBSTR(result_hex, -40)))
            ELSE NULL
        END AS token_address
    FROM
        gem_calls
    WHERE
        api_response:data:result IS NOT NULL
        AND api_response:data:result::STRING != '0x'
)

final AS (
    SELECT
        p.join_address,
        p.token_address
    FROM
        parsed_joins p
    WHERE
        p.token_address IS NOT NULL
        AND p.token_address != '0x0000000000000000000000000000000000000000'
UNION ALL
SELECT
    '0x37305b1cd40574e4c5ce33f8e8306be057fd7341' AS join_address, -- Sky: PSM
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS token_address, -- USDC
)

SELECT
    join_address,
    token_address,
    'sky' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(['join_address']) }} AS sky_v1_collateral_joins_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final