{{ config(
    materialized = 'incremental',
    unique_key = "compound_market_address",
    tags = ['silver','defi','lending','curated','compound','compound_v3']
) }}

{# Get variables #}
{% set vars = return_vars() %}

WITH origin_from_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'comp_v3_origin_from_address'
),
contracts_dim AS (
        SELECT
            address,
            name,
            symbol,
            decimals
        FROM
            {{ ref('core__dim_contracts') }}
    ),

    comp_v3_base AS (
        SELECT
            contract_address,
            origin_from_address,
            block_number,
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
                            'to': contract_address, 
                            'from': null, 
                            'data': RPAD('0xc55dae63', 64, '0')
                        }, 
                        utils.udf_int_to_hex(block_number)
                    ],
                    concat_ws('-', contract_address, '0xc55dae63', block_number)
                ),
                '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
            ) AS api_response
        FROM
            {{ ref('core__fact_event_logs') }}
        WHERE
            topic_0 = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'
            AND origin_from_address IN (
                SELECT
                    contract_address
                FROM
                    origin_from_addresses
            )

        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}' FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
        and contract_address not in (select compound_market_address from {{ this }})
        {% endif %}

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY contract_address
            ORDER BY block_number DESC
        ) = 1
    ),

    comp_v3_data AS (
        SELECT
            l.contract_address AS ctoken_address,
            l.origin_from_address,
            c1.symbol AS ctoken_symbol,
            c1.name AS ctoken_name,
            c1.decimals AS ctoken_decimals,
            LOWER(
                CONCAT(
                    '0x',
                    SUBSTR(
                        l.api_response:data:result :: STRING,
                        -40
                    )
                )
            ) AS underlying_address,
            c2.name AS underlying_name,
            c2.symbol AS underlying_symbol,
            c2.decimals AS underlying_decimals,
            l.block_number AS created_block,
            'Compound V3' AS compound_version
        FROM comp_v3_base l
        LEFT JOIN contracts_dim c1 ON l.contract_address = c1.address
        LEFT JOIN contracts_dim c2 ON LOWER(
            CONCAT(
                '0x',
                SUBSTR(
                    l.api_response:data:result :: STRING,
                    -40
                )
            )
        ) = c2.address
        WHERE c1.name IS NOT NULL
    )

    SELECT
        ctoken_address AS compound_market_address,
        ctoken_symbol AS compound_market_symbol,
        ctoken_name AS compound_market_name,
        ctoken_decimals AS compound_market_decimals,
        underlying_address AS underlying_asset_address,
        underlying_name AS underlying_asset_name,
        created_block AS created_block_number,
        origin_from_address,
        o.protocol,
        o.version,
        o.protocol || '-' || o.version AS platform,
        compound_version,
        {{ dbt_utils.generate_surrogate_key(['compound_market_address']) }} AS comp_asset_details_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        comp_v3_data c
LEFT JOIN origin_from_addresses o
    ON c.origin_from_address = o.contract_address