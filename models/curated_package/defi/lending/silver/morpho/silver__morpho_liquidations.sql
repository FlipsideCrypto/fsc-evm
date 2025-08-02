{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

{# Get variables #}
{% set vars = return_vars() %}

WITH morpho_blue_addresses AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_LENDING_CONTRACT_MAPPING
    ) }}
    WHERE
        type = 'morpho_blue_address'
),

traces AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        LEFT(
            input,
            10
        ) AS function_sig,
        len(input) AS segmented_input_len,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        CONCAT('0x', SUBSTR(segmented_input [0] :: STRING, 25)) AS loan_token,
        CONCAT('0x', SUBSTR(segmented_input [1] :: STRING, 25)) AS collateral_token,
        CONCAT('0x', SUBSTR(segmented_input [2] :: STRING, 25)) AS oracle_address,
        CONCAT('0x', SUBSTR(segmented_input [3] :: STRING, 25)) AS irm_address,
        CONCAT('0x', SUBSTR(segmented_input [5] :: STRING, 25)) AS borrower,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index
        ) AS trace_index_order,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        to_address IN (
            SELECT
                contract_address
            FROM
                morpho_blue_addresses
        )
        AND function_sig = '0xd8eabcb8'
        AND trace_succeeded
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
logs AS(
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        ROW_NUMBER() over (
            PARTITION BY l.tx_hash
            ORDER BY
                l.event_index
        ) AS event_index_order,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repay_assets,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS seized_assets,
        COALESCE(
            l.origin_to_address,
            l.contract_address
        ) AS lending_pool_contract,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        topics [0] :: STRING = '0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41'
        AND l.contract_address IN (
            SELECT
                contract_address
            FROM
                morpho_blue_addresses
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                traces
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    l.contract_address AS protocol_market,
    l.caller AS liquidator,
    l.borrower,
    t.loan_token AS debt_token,
    c0.token_symbol AS debt_token_symbol,
    l.repay_assets AS repaid_amount_unadj,
    l.repay_assets / pow(
        10,
        c0.token_decimals
    ) AS repaid_amount,
    t.collateral_token AS collateral_token,
    c1.token_symbol AS collateral_token_symbol,
    l.seized_assets AS liquidated_amount_unadj,
    l.seized_assets / pow(
        10,
        c1.token_decimals
    ) AS liquidated_amount,
    m.protocol || '-' || m.version AS platform,
    m.protocol,
    m.version,
    t._call_id AS _id,
    t._inserted_timestamp
FROM
    traces t
    INNER JOIN logs l
    ON l.tx_hash = t.tx_hash
    AND l.event_index_order = t.trace_index_order
    LEFT JOIN {{ ref('silver__contracts') }}
    c0
    ON c0.contract_address = t.loan_token
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON c1.contract_address = t.collateral_token
    LEFT JOIN morpho_blue_addresses m
    ON m.contract_address = l.contract_address
