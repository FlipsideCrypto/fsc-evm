{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% set nft_full_refresh = var('NFT_FULL_REFRESH', false) %}
{% set unique_key = "tx_hash" if uses_receipts_by_hash else "block_number" %}
{% set post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)' %}

{% if not nft_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = nft_full_refresh,
    post_hook = post_hook,
    tags = ['nft_core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    post_hook = post_hook,
    tags = ['nft_core']
) }}

{% endif %}

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
        case 
            when topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' then 'erc721_Transfer'
            when topic_0 :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' then 'erc1155_TransferSingle'
            when topic_0 :: STRING = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb' then 'erc1155_TransferBatch'
        end as token_transfer_type,
        case 
            when token_transfer_type = 'erc721_Transfer' then CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40)) 
            when token_transfer_type = 'erc1155_TransferSingle' OR token_transfer_type = 'erc1155_TransferBatch' then CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40))
        end as from_address,
        case 
            when token_transfer_type = 'erc721_Transfer' then CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) 
            when token_transfer_type = 'erc1155_TransferSingle' OR token_transfer_type = 'erc1155_TransferBatch' then CONCAT('0x', SUBSTR(topic_3 :: STRING, 27, 40))
        end as to_address,
        case 
            when token_transfer_type = 'erc721_Transfer' then utils.udf_hex_to_int(topic_3 :: STRING) :: STRING 
            when token_transfer_type = 'erc1155_TransferSingle' then utils.udf_hex_to_int(segmented_data [0] :: STRING) :: STRING
        end as token_id,
        case
            when token_transfer_type = 'erc721_Transfer' then null 
            when token_transfer_type = 'erc1155_TransferSingle' then utils.udf_hex_to_int(segmented_data [1] :: STRING) :: STRING
        end as erc1155_value,
        case 
            when token_transfer_type = 'erc721_Transfer' then null 
            when token_transfer_type = 'erc1155_TransferSingle' OR token_transfer_type = 'erc1155_TransferBatch' then CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40))
        end as operator_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        tx_succeeded
        and not event_removed
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
AND TO_TIMESTAMP_NTZ(modified_timestamp) >   (
    SELECT
        MAX(modified_timestamp)
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
        q.quantity AS erc1155_value,
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
        erc1155_value,
        1 AS intra_event_index,
        token_transfer_type
    FROM
        base
    where 
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
        erc1155_value,
        intra_event_index,
        'erc1155_TransferBatch' AS token_transfer_type
    FROM
        transfer_batch_final
    WHERE
        erc1155_value != '0'
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
        erc1155_value,
        intra_event_index,
        token_transfer_type,
        name AS project_name,
        CASE
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint'
            ELSE 'other'
        END AS event_type,
        case when token_transfer_type = 'erc721_Transfer' then 'erc721'
        when token_transfer_type = 'erc1155_TransferSingle' then 'erc1155'
        when token_transfer_type = 'erc1155_TransferBatch' then 'erc1155'
        end as token_standard
    FROM
        all_transfers A
        LEFT JOIN {{ ref('core__dim_contracts') }} C 
        on a.contract_address = c.address and c.name IS NOT NULL
    WHERE
        to_address IS NOT NULL
)
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
    erc1155_value,
    intra_event_index,
    token_transfer_type,
    project_name,
    event_type,
    token_standard,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','intra_event_index']
    ) }} AS ez_nft_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    final_transfers

{% if is_incremental() %}
SELECT 
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.tx_position,
    t.event_index,
    t.origin_function_signature,
    t.origin_from_address,
    t.origin_to_address,    
    t.contract_address,
    t.from_address,
    t.to_address,
    t.token_id,
    t.erc1155_value,
    t.intra_event_index,
    t.token_transfer_type,
    c.name AS project_name_heal,
    t.event_type,
    t.token_standard,
    t.ez_nft_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM {{ this }} t 
INNER JOIN {{ ref('core__dim_contracts') }} c
    ON t.contract_address = c.address
    AND c.name IS NOT NULL
    and c.modified_timestamp > current_date() - 30
WHERE t.project_name IS NULL and t.modified_timestamp > current_date() - 30
and t.inserted_timestamp < (SELECT max(inserted_timestamp) FROM {{ this }})
{% endif %}