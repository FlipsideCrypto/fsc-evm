{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_NFT_TRANSFERS_UNIQUE_KEY,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)",
    tags = ['gold','nft','ez','phase_2']
) }}

WITH base AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_position,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'erc721_Transfer'
            WHEN topic_0 :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' THEN 'erc1155_TransferSingle'
            WHEN topic_0 :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb' THEN 'erc1155_TransferBatch'
        END AS token_transfer_type,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40))
            WHEN token_transfer_type = 'erc1155_TransferSingle'
            OR token_transfer_type = 'erc1155_TransferBatch' THEN CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40))
        END AS from_address,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40))
            WHEN token_transfer_type = 'erc1155_TransferSingle'
            OR token_transfer_type = 'erc1155_TransferBatch' THEN CONCAT('0x', SUBSTR(topic_3 :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN utils.udf_hex_to_int(
                topic_3 :: STRING
            ) :: STRING
            WHEN token_transfer_type = 'erc1155_TransferSingle' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: STRING
        END AS token_id,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN NULL
            WHEN token_transfer_type = 'erc1155_TransferSingle' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: STRING
        END AS quantity,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN NULL
            WHEN token_transfer_type = 'erc1155_TransferSingle'
            OR token_transfer_type = 'erc1155_TransferBatch' THEN CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40))
        END AS operator_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        tx_succeeded
        AND NOT event_removed
        AND (
            (
                topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND DATA = '0x'
                AND topic_3 IS NOT NULL
            ) --erc721s
            OR (
                topic_0 :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) --erc1155s
            OR (
                topic_0 :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
            ) --erc1155s TransferBatch event
        )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
transfer_batch_raw AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_position,
        segmented_data,
        operator_address,
        from_address,
        to_address,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS tokenid_length
    FROM
        base
    WHERE
        topic_0 :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
        AND to_address IS NOT NULL
),
flattened AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_position,
        segmented_data,
        operator_address,
        from_address,
        to_address,
        INDEX,
        VALUE,
        tokenid_length,
        2 + tokenid_length AS tokenid_indextag,
        4 + tokenid_length AS quantity_indextag_start,
        4 + tokenid_length + tokenid_length AS quantity_indextag_end,
        CASE
            WHEN INDEX BETWEEN 3
            AND (
                tokenid_indextag
            ) THEN 'tokenid'
            WHEN INDEX BETWEEN (
                quantity_indextag_start
            )
            AND (
                quantity_indextag_end
            ) THEN 'quantity'
            ELSE NULL
        END AS label
    FROM
        transfer_batch_raw,
        LATERAL FLATTEN (
            input => segmented_data
        )
),
tokenid_list AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_position,
        segmented_data,
        operator_address,
        from_address,
        to_address,
        utils.udf_hex_to_int(
            VALUE :: STRING
        ) :: STRING AS tokenId,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                INDEX ASC
        ) AS tokenid_order
    FROM
        flattened
    WHERE
        label = 'tokenid'
),
quantity_list AS (
    SELECT
        tx_hash,
        event_index,
        utils.udf_hex_to_int(
            VALUE :: STRING
        ) :: STRING AS quantity,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            event_index
            ORDER BY
                INDEX ASC
        ) AS quantity_order
    FROM
        flattened
    WHERE
        label = 'quantity'
),
transfer_batch_final AS (
    SELECT
        block_number,
        block_timestamp,
        t.tx_hash,
        t.event_index,
        contract_address,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_position,
        segmented_data,
        operator_address,
        from_address,
        to_address,
        t.tokenId AS token_id,
        q.quantity AS quantity,
        tokenid_order AS intra_event_index
    FROM
        tokenid_list t
        INNER JOIN quantity_list q
        ON t.tx_hash = q.tx_hash
        AND t.event_index = q.event_index
        AND t.tokenid_order = q.quantity_order
),
all_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        token_id,
        quantity,
        1 AS intra_event_index,
        token_transfer_type
    FROM
        base
    WHERE
        token_transfer_type <> 'erc1155_TransferBatch'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        token_id,
        quantity,
        intra_event_index,
        'erc1155_TransferBatch' AS token_transfer_type
    FROM
        transfer_batch_final
),
final_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        token_id,
        COALESCE(
            quantity,
            '1'
        ) AS quantity,
        intra_event_index,
        token_transfer_type,
        NAME AS NAME,
        from_address = '0x0000000000000000000000000000000000000000' AS is_mint,
        CASE
            WHEN token_transfer_type = 'erc721_Transfer' THEN 'erc721'
            WHEN token_transfer_type = 'erc1155_TransferSingle' THEN 'erc1155'
            WHEN token_transfer_type = 'erc1155_TransferBatch' THEN 'erc1155'
        END AS token_standard,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }} AS ez_nft_transfers_id,
        {% if is_incremental() %}
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
        {% else %}
        CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '4 hours' THEN SYSDATE() 
            ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
        CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '4 hours' THEN SYSDATE() 
            ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
        {% endif %}
    FROM
        all_transfers A
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON A.contract_address = C.address
        AND C.name IS NOT NULL
    WHERE
        to_address IS NOT NULL
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        intra_event_index,
        token_transfer_type,
        is_mint,
        from_address,
        to_address,
        contract_address,
        token_id,
        quantity,
        token_standard,
        NAME,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        ez_nft_transfers_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        final_transfers

{% if is_incremental() %}
UNION ALL
SELECT
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.tx_position,
    t.event_index,
    t.intra_event_index,
    t.token_transfer_type,
    t.is_mint,
    t.from_address,
    t.to_address,
    t.contract_address,
    t.token_id,
    t.quantity,
    t.token_standard,
    C.name,
    t.origin_function_signature,
    t.origin_from_address,
    t.origin_to_address,
    t.ez_nft_transfers_id,
    {% if is_incremental() %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN t.block_timestamp >= date_trunc('hour',SYSDATE()) - interval '4 hours' THEN SYSDATE() 
        ELSE GREATEST(t.block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN t.block_timestamp >= date_trunc('hour',SYSDATE()) - interval '4 hours' THEN SYSDATE() 
        ELSE GREATEST(t.block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    {{ this }}
    t
    INNER JOIN {{ ref('core__dim_contracts') }} C
    ON t.contract_address = C.address
    AND C.name IS NOT NULL
    AND C.modified_timestamp > CURRENT_DATE() - 30
    LEFT JOIN final_transfers f USING (ez_nft_transfers_id)
WHERE
    t.name IS NULL
    AND f.ez_nft_transfers_id IS NULL
{% endif %}
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        event_index,
        intra_event_index
        ORDER BY
            modified_timestamp DESC
    ) = 1
