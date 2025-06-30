{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['contract_address'],
    incremental_strategy = 'delete+insert',
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','phase_4']
) }}

WITH verified_assets AS (

    SELECT
        token_address AS contract_address
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
        AND asset_id IS NOT NULL
        AND token_address IS NOT NULL
),
erc20_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        TRY_TO_NUMBER(raw_amount_precise) AS raw_amount,
        C.decimals,
        tx_succeeded
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN verified_assets v --limit balances to verified assets only
        USING (contract_address)
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON l.contract_address = C.address
        AND C.decimals IS NOT NULL
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND DATA IS NOT NULL
        AND raw_amount IS NOT NULL
        AND l.block_timestamp > DATEADD('day', -31, SYSDATE())

{% if is_incremental() %}
AND l.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
AND l.contract_address NOT IN (
    SELECT
        DISTINCT contract_address
    FROM
        {{ this }}
    WHERE
        slot_number_array IS NOT NULL --only attempt to map again if slot is missing
)
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY l.contract_address
ORDER BY
    block_number DESC)) = 1 --only keep the latest transfer for each contract
),
wrapped_native_transfers AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        IFF(
            topic_0 = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            '0x' || SUBSTR(
                topic_1 :: STRING,
                27
            ),
            '0x0000000000000000000000000000000000000000'
        ) AS from_address,
        IFF(
            topic_0 = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            '0x0000000000000000000000000000000000000000',
            '0x' || SUBSTR(
                topic_1 :: STRING,
                27
            )
        ) AS to_address,
        contract_address,
        TRY_TO_NUMBER(utils.udf_hex_to_int(DATA)) AS raw_amount,
        18 AS decimals
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN verified_assets v USING (contract_address)
    WHERE
        block_timestamp > DATEADD('day', -31, SYSDATE())
        AND topic_0 IN (
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            -- withdraw
            '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' --deposit
        )

{% if is_incremental() %}
AND l.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        DISTINCT contract_address
    FROM
        {{ this }}
    WHERE
        slot_number_array IS NOT NULL
)
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY topic_0
ORDER BY
    block_number DESC)) = 1 --keep the latest event for each topic
),
transfer_direction AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        to_address AS address,
        contract_address,
        raw_amount,
        decimals
    FROM
        erc20_transfers
    UNION ALL
    SELECT
        block_number,
        tx_position,
        tx_hash,
        from_address AS address,
        contract_address,
        (
            -1 * raw_amount
        ) AS raw_amount,
        decimals
    FROM
        erc20_transfers
    UNION ALL
    SELECT
        block_number,
        tx_position,
        tx_hash,
        to_address AS address,
        contract_address,
        raw_amount,
        decimals
    FROM
        wrapped_native_transfers
    UNION ALL
    SELECT
        block_number,
        tx_position,
        tx_hash,
        from_address AS address,
        contract_address,
        (
            -1 * raw_amount
        ) AS raw_amount,
        decimals
    FROM
        wrapped_native_transfers
),
direction_agg AS (
    SELECT
        block_number,
        tx_hash,
        tx_position,
        address,
        contract_address,
        SUM(raw_amount) AS transfer_amount,
        MAX(decimals) AS decimals
    FROM
        transfer_direction
    GROUP BY
        ALL
),
state_tracer AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        pre_state_json,
        post_state_json,
        address,
        pre_storage,
        post_storage
    FROM
        {{ ref('silver__state_tracer') }}
    WHERE
        block_number IN (
            SELECT
                DISTINCT block_number
            FROM
                erc20_transfers
            UNION ALL
            SELECT
                DISTINCT block_number
            FROM
                wrapped_native_transfers
        ) --only include blocks with relevant transfers

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
    FROM
        {{ this }})
    {% endif %}
),
pre_state_storage AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        pre_state_json,
        address,
        pre_storage,
        pre.key :: STRING AS storage_key,
        pre.value :: STRING AS pre_storage_value_hex
    FROM
        state_tracer,
        LATERAL FLATTEN(
            input => pre_storage
        ) pre
),
post_state_storage AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        post_state_json,
        address,
        post_storage,
        post.key :: STRING AS storage_key,
        post.value :: STRING AS post_storage_value_hex
    FROM
        state_tracer,
        LATERAL FLATTEN(
            input => post_storage
        ) post
),
state_storage AS (
    SELECT
        block_number,
        COALESCE(
            pre.tx_position,
            post.tx_position
        ) AS tx_position,
        COALESCE(
            pre.tx_hash,
            post.tx_hash
        ) AS tx_hash,
        COALESCE(
            pre.address,
            post.address
        ) AS contract_address,
        COALESCE(
            pre.storage_key,
            post.storage_key
        ) AS storage_key,
        COALESCE(
            pre_storage_value_hex,
            '0x0000000000000000000000000000000000000000000000000000000000000000'
        ) AS pre_storage_hex,
        COALESCE(
            post_storage_value_hex,
            '0x0000000000000000000000000000000000000000000000000000000000000000'
        ) AS post_storage_hex
    FROM
        pre_state_storage pre full
        OUTER JOIN post_state_storage post USING (
            block_number,
            tx_position,
            address,
            storage_key
        )
),
num_generator AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                1 ASC
        ) - 1 AS rn
    FROM
        TABLE(GENERATOR(rowcount => 51)) {# no theoretical limit on max slots for erc20, 2-15 is common. Can reduce if needed. #}
),
transfer_mapping AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        contract_address,
        address,
        utils.udf_mapping_slot(
            address,
            rn
        ) AS storage_key,
        rn AS slot_number,
        transfer_amount,
        decimals
    FROM
        direction_agg,
        num_generator
),
balances AS (
    SELECT
        block_number,
        tx_position,
        tx_hash,
        contract_address,
        address,
        storage_key,
        slot_number,
        pre_storage_hex AS pre_balance_hex,
        utils.udf_hex_to_int(pre_storage_hex) AS pre_balance_raw,
        utils.udf_decimal_adjust(
            pre_balance_raw,
            decimals
        ) AS pre_balance_precise,
        pre_balance_precise :: FLOAT AS pre_balance,
        post_storage_hex AS post_balance_hex,
        utils.udf_hex_to_int(post_storage_hex) AS post_balance_raw,
        utils.udf_decimal_adjust(
            post_balance_raw,
            decimals
        ) AS post_balance_precise,
        post_balance_precise :: FLOAT AS post_balance,
        TRY_TO_NUMBER(post_balance_raw) - TRY_TO_NUMBER(pre_balance_raw) AS net_balance_raw,
        post_balance_precise - pre_balance_precise AS net_balance,
        transfer_amount,
        decimals
    FROM
        state_storage
        INNER JOIN transfer_mapping USING (
            block_number,
            tx_position,
            contract_address,
            storage_key
        )
    WHERE
        net_balance_raw = transfer_amount
),
FINAL AS (
    SELECT
        contract_address,
        MAX(block_number) AS max_block_number,
        ARRAY_AGG(
            DISTINCT slot_number
        ) AS slot_number_array
    FROM
        balances
    GROUP BY
        contract_address
)
SELECT
    contract_address,
    max_block_number,
    slot_number_array,
    TRY_TO_NUMBER(
        slot_number_array [0] :: STRING
    ) AS slot_number,
    ARRAY_SIZE(slot_number_array) AS num_slots,
    {{ dbt_utils.generate_surrogate_key(['contract_address']) }} AS balance_slots_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
-- This model determines the balanceOf slot for each contract based on matching an erc20 token transfer with state data.
-- NULL slot indicates that the contract does not have a balanceOf slot.
-- >1 slot indicates that the contract has multiple balanceOf slots.
-- Logic for these contracts must be handled separately (e.g. rebase tokens, wrapped assets etc.)
