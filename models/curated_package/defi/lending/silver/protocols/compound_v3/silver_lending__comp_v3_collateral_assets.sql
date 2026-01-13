{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = "collateral_asset_id",
    tags = ['silver','defi','lending','curated','compound','comp_v3']
) }}

WITH
-- Generate indices 0-14 for each market (max expected collateral assets)
indices AS (
    SELECT column1 AS idx FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14))
),

market_indices AS (
    SELECT
        m.compound_market_address,
        m.protocol,
        m.version,
        m.platform,
        i.idx
    FROM
        {{ ref('silver_lending__comp_v3_asset_details') }} m
    CROSS JOIN
        indices i
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} t
        WHERE t.compound_market_address = m.compound_market_address
        AND t.asset_index = i.idx
    )
    {% endif %}
),

-- Call getAssetInfo(i) for each market/index combination
asset_info_calls AS (
    SELECT
        compound_market_address,
        protocol,
        version,
        platform,
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
                        'to': compound_market_address,
                        'from': null,
                        'data': CONCAT('0xc8c7fe6b', LPAD(utils.udf_int_to_hex(idx), 64, '0'))
                    },
                    'latest'
                ],
                concat_ws('-', compound_market_address, 'getAssetInfo', idx)
            ),
            '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS api_response
    FROM
        market_indices
),

-- getAssetInfo returns tuple: (uint8 offset, address asset, address priceFeed, ...)
parsed_assets AS (
    SELECT
        compound_market_address,
        protocol,
        version,
        platform,
        idx AS asset_index,
        api_response:data:result::STRING AS result_hex,
        CASE
            WHEN result_hex IS NOT NULL
            AND LENGTH(result_hex) >= 130
            AND result_hex != '0x'
            THEN LOWER(CONCAT('0x', SUBSTR(result_hex, 27, 40)))
            ELSE NULL
        END AS collateral_asset_address
    FROM
        asset_info_calls
    WHERE
        api_response:data:result IS NOT NULL
        AND api_response:data:result::STRING != '0x'
        AND LENGTH(api_response:data:result::STRING) >= 130
)

SELECT
    compound_market_address,
    collateral_asset_address,
    asset_index,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(['compound_market_address', 'collateral_asset_address']) }} AS collateral_asset_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed_assets
WHERE
    collateral_asset_address IS NOT NULL
    AND collateral_asset_address != '0x0000000000000000000000000000000000000000'
