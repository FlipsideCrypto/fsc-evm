{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_BRIDGE_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'circle_cctp'
        AND version = 'v1'
),
-- to exclude circle transactions
raw_traces AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        output,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS part,
        LEFT(
            input,
            10
        ) AS function_sig,
        trace_address,
        REGEXP_REPLACE(
            trace_address,
            '_[0-9]+_[0-9]+$',
            ''
        ) AS parent_address,
        c.contract_address, 
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
        t
        LEFT JOIN contract_mapping C
        ON to_address = contract_address
    WHERE
        block_timestamp :: DATE >= '2023-10-01'
        AND tx_succeeded
        AND trace_succeeded
        AND t.type = 'CALL'
        AND (
            (
                C.contract_address IS NOT NULL
                AND function_sig IN (
                    '0xf856ddb6',
                    '0x6fd3504e'
                )
            ) -- circle cctp v1 address & functions - depositForBurn, depositForBurnWithCaller
            OR (
                function_sig = '0xdf0aa9e9'
            ) -- forwardFromRouter
        )
),
circle_calls AS (
    SELECT
        tx_hash,
        trace_index AS circle_trace_index,
        from_address,
        to_address,
        trace_address,
        parent_address AS circle_parent_address
    FROM
        raw_traces
    WHERE
        contract_address IS NOT NULL
        AND function_sig IN (
            '0xf856ddb6',
            '0x6fd3504e'
        )
),
circle_exclusion_join AS (
    SELECT
        C.tx_hash,
        circle_trace_index,
        r.trace_index AS parent_trace_index
    FROM
        circle_calls C
        INNER JOIN raw_traces r
        ON C.circle_parent_address = r.trace_address
        AND C.tx_hash = r.tx_hash
    WHERE
        r.function_sig = '0xdf0aa9e9' --forwardFromRouter
),
ccip_decoded AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        t.tx_hash,
        input,
        part,
        utils.udf_hex_to_int(
            part [0] :: STRING
        ) :: STRING AS dest_chain_selector,
        utils.udf_hex_to_int(
            part [2] :: STRING
        ) :: INT AS fee_token_amount,
        '0x' || SUBSTR(
            part [3] :: STRING,
            25
        ) AS original_sender,
        utils.udf_hex_to_int(
            part [4] :: STRING
        ) :: INT / 32 AS offset_receiver,
        utils.udf_hex_to_int(
            part [offset_receiver + 4] :: STRING
        ) :: INT * 2 AS receiver_length,
        (
            offset_receiver + 5
        ) * 64 AS receiver_byteskip,
        SUBSTR(input, (11 + receiver_byteskip), receiver_length) AS receiver_raw,
        '0x' || SUBSTR(
            receiver_raw,
            25,
            40
        ) AS receiver_evm,
        utils.udf_hex_to_int(
            part [6] :: STRING
        ) :: INT / 32 AS offset_token_amount,
        utils.udf_hex_to_int(
            part [offset_token_amount + 4] :: STRING
        ) :: INT AS token_amount_array,
        chain_name,
        trace_index,
        from_address,
        contract_address,
        bridge_address,
        protocol,
        version,
        TYPE,
        platform,
        t.modified_timestamp,
        ROW_NUMBER() over (
            ORDER BY
                trace_index ASC
        ) AS rn
    FROM
        raw_traces t
        INNER JOIN {{ ref('silver_bridge__ccip_on_ramp_address') }}
        ON to_address = on_ramp_address
    WHERE
        function_sig = '0xdf0aa9e9'
),
tokens_raw AS (
    SELECT
        tx_hash,
        trace_index,
        INDEX,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            trace_index
            ORDER BY
                INDEX ASC
        ) - 1 AS row_num,
        TRUNC(
            row_num / 2
        ) AS GROUPING,
        VALUE :: STRING AS VALUE
    FROM
        ccip_decoded,
        LATERAL FLATTEN (
            input => part
        )
    WHERE
        INDEX BETWEEN (
            offset_token_amount + 5
        )
        AND (offset_token_amount + 5 + (2 * token_amount_array) - 1)
),
token_grouping AS (
    SELECT
        tx_hash,
        trace_index,
        GROUPING,
        ARRAY_AGG(VALUE) within GROUP (
            ORDER BY
                INDEX ASC
        ) AS token_array
    FROM
        tokens_raw
    GROUP BY
        ALL
),
final_ccip AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        trace_index,
        GROUPING,
        '0x' || SUBSTR(
            token_array [0] :: STRING,
            25
        ) AS token_address,
        utils.udf_hex_to_int(
            token_array [1] :: STRING
        ) :: INT AS amount_unadj,
        dest_chain_selector,
        receiver_raw,
        receiver_evm,
        chain_name,
        protocol,
        version,
        TYPE,
        platform,
        contract_address,
        bridge_address,
        modified_timestamp 
    FROM
        ccip_decoded
        INNER JOIN token_grouping USING (
            tx_hash,
            trace_index
        )
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    f.tx_hash,
    trace_index,
    grouping,
    NULL AS event_index,
    bridge_address,
    NULL AS event_name,
    origin_from_address AS sender,
    receiver_evm AS receiver,
    receiver_evm AS destination_chain_receiver,
    dest_chain_selector :: STRING AS destination_chain_id,
    chain_name AS destination_chain,
    token_address,
    amount_unadj,
    platform,
    protocol,
    version,
    TYPE,
    circle_trace_index,
    parent_trace_index,
    CONCAT(
        f.tx_hash,
        '-',
        trace_index,
        '-',
        GROUPING
    ) AS _id,
    modified_timestamp
FROM
    final_ccip f
    LEFT JOIN circle_exclusion_join C
    ON f.tx_hash = C.tx_hash
    AND f.trace_index = C.parent_trace_index
WHERE
    parent_trace_index IS NULL
