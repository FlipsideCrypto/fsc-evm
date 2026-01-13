{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "comp_v1_asset_details_id",
    tags = ['silver','defi','lending','curated','compound','comp_v1']
) }}

-- Compound V1 is Ethereum-only with a single MoneyMarket contract
-- This model discovers collateral markets via collateralMarkets(uint256) calls

-- Generate indices 0-19 for collateral markets (max expected)
WITH indices AS (
    SELECT column1 AS idx FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19))
),

market_indices AS (
    SELECT idx
    FROM indices
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} t
        WHERE t.market_index = indices.idx
    )
    {% endif %}
),

-- Call collateralMarkets(i) for each index
-- Function selector: collateralMarkets(uint256) = 0x5e9a523c
collateral_market_calls AS (
    SELECT
        '0x3fda67f7583380e67ef93072294a7fac882fd7e7' AS money_market_address,
        idx,
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
                        'to': money_market_address,
                        'from': null,
                        'data': CONCAT('0x5e9a523c', LPAD(utils.udf_int_to_hex(idx), 64, '0'))
                    },
                    'latest'
                ],
                concat_ws('-', money_market_address, 'collateralMarkets', idx)
            ),
            '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS api_response
    FROM
        market_indices
),

parsed_markets AS (
    SELECT
        money_market_address,
        idx AS market_index,
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
        collateral_market_calls
    WHERE
        api_response:data:result IS NOT NULL
        AND api_response:data:result::STRING != '0x'
)

SELECT
    p.money_market_address,
    p.token_address,
    c.name AS token_name,
    c.symbol AS token_symbol,
    c.decimals AS token_decimals,
    p.market_index,
    'compound' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(['token_address']) }} AS comp_v1_asset_details_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed_markets p
LEFT JOIN {{ ref('core__dim_contracts') }} c ON p.token_address = c.address
WHERE
    p.token_address IS NOT NULL
    AND p.token_address != '0x0000000000000000000000000000000000000000'
