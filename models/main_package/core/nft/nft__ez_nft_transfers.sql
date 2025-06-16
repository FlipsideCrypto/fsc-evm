{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_nft_transfers_id',
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature), SUBSTRING(origin_from_address, origin_to_address, from_address, to_address, origin_function_signature)",
    tags = ['gold','core','ez','phase_2']
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
            {% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
                OR (
                    topic_0 :: STRING IN (
                        '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                        -- regular transfer topic
                        '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3',
                        -- PunkBought
                        '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8' -- PunkTransfer
                    )
                    AND contract_address IN (
                        '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                        -- Old V1
                        '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' -- cryptopunks
                    )
                )
                OR (
                    -- legacy tokens
                    topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                    AND topic_1 IS NULL
                )
            {% endif %}
        )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }})
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
{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    punks_bought_raw AS (
        -- punks bought via sale or bids
        SELECT
            block_number,
            block_timestamp,
            tx_hash,
            contract_address,
            topic_1,
            topic_0,
            topic_2,
            utils.udf_hex_to_int(
                topic_1 :: STRING
            ) :: STRING AS token_id,
            CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) AS from_address,
            CONCAT('0x', SUBSTR(topic_3 :: STRING, 27, 40)) AS to_address,
            '1' AS quantity,
            LAG(topic_0) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ) AS prev_topic_0,
            LAG(topic_1) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ) AS prev_topic_1,
            LAG(topic_2) over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ) AS prev_topic_2,
            CONCAT('0x', SUBSTR(prev_topic_1 :: STRING, 27, 40)) AS prev_from_address,
            CONCAT('0x', SUBSTR(prev_topic_2 :: STRING, 27, 40)) AS prev_to_address,
            tx_position,
            event_index,
            origin_function_signature,
            origin_from_address,
            origin_to_address
        FROM
            base
        WHERE
            topic_0 :: STRING IN (
                '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3',
                -- punk bought
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- transfer
            )
            AND contract_address IN (
                '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                -- Old V1
                '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' -- cryptopunks
            )
    ),
    punks_bought AS (
        SELECT
            block_number,
            tx_hash,
            block_timestamp,
            contract_address,
            token_id,
            from_address,
            CASE
                WHEN to_address = '0x0000000000000000000000000000000000000000'
                AND prev_topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND prev_from_address = from_address THEN prev_to_address
                ELSE to_address
            END AS to_address,
            quantity,
            tx_position,
            event_index,
            origin_function_signature,
            origin_from_address,
            origin_to_address
        FROM
            punks_bought_raw
        WHERE
            topic_0 :: STRING = '0x58e5d5a525e3b40bc15abaa38b5882678db1ee68befd2f60bafe3a7fd06db9e3'
    ),
    punks_transfer AS (
        SELECT
            block_number,
            tx_hash,
            block_timestamp,
            contract_address,
            utils.udf_hex_to_int(
                DATA :: STRING
            ) :: STRING AS token_id,
            CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40)) AS from_address,
            CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) AS to_address,
            '1' AS quantity,
            event_index,
            tx_position,
            origin_function_signature,
            origin_from_address,
            origin_to_address
        FROM
            base
        WHERE
            topic_0 :: STRING = '0x05af636b70da6819000c49f85b21fa82081c632069bb626f30932034099107d8'
            AND contract_address IN (
                '0x6ba6f2207e343923ba692e5cae646fb0f566db8d',
                -- Old V1
                '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' -- cryptopunks
            )
    ),
    legacy_tokens AS (
        SELECT
            block_number,
            tx_hash,
            block_timestamp,
            contract_address,
            CONCAT('0x', SUBSTR(segmented_data [0], 25, 40)) AS from_address,
            CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) AS to_address,
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: STRING AS token_id,
            '1' AS quantity,
            tx_position,
            event_index,
            origin_function_signature,
            origin_from_address,
            origin_to_address
        FROM
            base
        WHERE
            topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND topic_1 IS NULL
    ),
{% endif %}

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
        (
            topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND DATA = '0x'
            AND topic_3 IS NOT NULL
        ) --erc721s TransferSingle event
        OR (
            topic_0 :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
        ) --erc1155s
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

        {% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
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
        1 AS intra_event_index,
        'cryptopunks_PunkTransfer' AS token_transfer_type
    FROM
        punks_transfer
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
        1 AS intra_event_index,
        'cryptopunks_PunkBought' AS token_transfer_type
    FROM
        punks_bought
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
        1 AS intra_event_index,
        'legacy_Transfer' AS token_transfer_type
    FROM
        legacy_tokens
    {% endif %}
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
            WHEN token_transfer_type = 'erc1155_TransferBatch' THEN 'erc1155' {% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
                WHEN token_transfer_type = 'legacy_Transfer' THEN 'legacy'
                WHEN token_transfer_type = 'cryptopunks_PunkBought' THEN 'cryptopunks'
                WHEN token_transfer_type = 'cryptopunks_PunkTransfer' THEN 'cryptopunks'
            {% endif %}
        END AS token_standard,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }} AS ez_nft_transfers_id,

{% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp
{% else %}
    CASE
        WHEN block_timestamp >= DATE_TRUNC('hour', SYSDATE()) - INTERVAL '6 hours' THEN SYSDATE()
        ELSE GREATEST(block_timestamp, DATEADD('day', -10, SYSDATE()))END AS inserted_timestamp,
        CASE
            WHEN block_timestamp >= DATE_TRUNC('hour', SYSDATE()) - INTERVAL '6 hours' THEN SYSDATE()
            ELSE GREATEST(block_timestamp, DATEADD('day', -10, SYSDATE()))END AS modified_timestamp
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
                COALESCE(
                    token_id,
                    '0'
                ) AS token_id,
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

{% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp
{% else %}
    CASE
        WHEN t.block_timestamp >= DATE_TRUNC('hour', SYSDATE()) - INTERVAL '6 hours' THEN SYSDATE()
        ELSE GREATEST(t.block_timestamp, DATEADD('day', -10, SYSDATE()))END AS inserted_timestamp,
        CASE
            WHEN t.block_timestamp >= DATE_TRUNC('hour', SYSDATE()) - INTERVAL '6 hours' THEN SYSDATE()
            ELSE GREATEST(t.block_timestamp, DATEADD('day', -10, SYSDATE()))END AS modified_timestamp
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
