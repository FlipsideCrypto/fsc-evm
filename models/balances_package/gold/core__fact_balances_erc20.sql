{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

--depends_on: {{ ref('core__fact_blocks') }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'round(block_number, -3)'],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','phase_4']
) }}

WITH verified_assets AS (

    SELECT
        token_address AS contract_address,
        slot_number
    FROM
        {{ ref('price__ez_asset_metadata') }}
        v
        INNER JOIN {{ ref('silver__balance_slots') }}
        s ON v.token_address = s.contract_address
    WHERE
        is_verified
        AND asset_id IS NOT NULL
        AND slot_number IS NOT NULL
        AND num_slots = 1 --only include contracts with a single balanceOf slot
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
        slot_number,
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

{% if is_incremental() %}
AND l.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
wrapped_native_transfers AS (
    SELECT
        block_number,
        block_timestamp,
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
        18 AS decimals,
        slot_number,
        tx_succeeded
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN verified_assets v USING (contract_address)
    WHERE
        topic_0 IN (
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
{% endif %}
),
transfer_direction AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        to_address AS address,
        contract_address,
        raw_amount,
        decimals,
        slot_number,
        tx_succeeded
    FROM
        erc20_transfers
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        from_address AS address,
        contract_address,
        (
            -1 * raw_amount
        ) AS raw_amount,
        decimals,
        slot_number,
        tx_succeeded
    FROM
        erc20_transfers
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        to_address AS address,
        contract_address,
        raw_amount,
        decimals,
        slot_number,
        tx_succeeded
    FROM
        wrapped_native_transfers
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        from_address AS address,
        contract_address,
        (
            -1 * raw_amount
        ) AS raw_amount,
        decimals,
        slot_number,
        tx_succeeded
    FROM
        wrapped_native_transfers
),
direction_agg AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        address,
        contract_address,
        tx_succeeded,
        SUM(raw_amount) AS transfer_amount,
        MAX(decimals) AS decimals,
        MAX(slot_number) AS slot_number
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
transfer_mapping AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        contract_address,
        address,
        slot_number,
        utils.udf_mapping_slot(
            address,
            slot_number
        ) AS storage_key,
        transfer_amount,
        decimals,
        tx_succeeded
    FROM
        direction_agg
),
balances AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        contract_address,
        address,
        storage_key,
        slot_number,
        pre_storage_hex AS pre_hex_balance,
        utils.udf_hex_to_int(pre_storage_hex) AS pre_raw_balance,
        utils.udf_decimal_adjust(
            pre_raw_balance,
            decimals
        ) AS pre_balance_precise,
        pre_balance_precise :: FLOAT AS pre_balance,
        post_storage_hex AS post_hex_balance,
        utils.udf_hex_to_int(post_storage_hex) AS post_raw_balance,
        utils.udf_decimal_adjust(
            post_raw_balance,
            decimals
        ) AS post_balance_precise,
        post_balance_precise :: FLOAT AS post_balance,
        TRY_TO_NUMBER(post_raw_balance) - TRY_TO_NUMBER(pre_raw_balance) AS net_raw_balance,
        post_balance_precise - pre_balance_precise AS net_balance,
        transfer_amount,
        decimals,
        tx_succeeded
    FROM
        state_storage
        INNER JOIN transfer_mapping USING (
            block_number,
            tx_position,
            contract_address,
            storage_key
        )
    WHERE
        net_raw_balance = transfer_amount
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp AS block_timestamp_heal,
        tx_position,
        tx_hash,
        tx_succeeded,
        contract_address,
        decimals,
        slot_number,
        address,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance_precise,
        pre_balance,
        post_hex_balance,
        post_raw_balance,
        post_balance_precise,
        post_balance,
        net_raw_balance,
        net_balance
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core__fact_blocks') }}
        b USING(block_number)
    WHERE
        t.block_timestamp IS NULL
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        tx_succeeded,
        contract_address,
        decimals,
        slot_number,
        address,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance_precise,
        pre_balance,
        post_hex_balance,
        post_raw_balance,
        post_balance_precise,
        post_balance,
        net_raw_balance,
        net_balance
    FROM
        balances

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
    tx_position,
    tx_hash,
    tx_succeeded,
    contract_address,
    decimals,
    slot_number,
    address,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance_precise,
    pre_balance,
    post_hex_balance,
    post_raw_balance,
    post_balance_precise,
    post_balance,
    net_raw_balance,
    net_balance
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_position,
    tx_hash,
    tx_succeeded,
    contract_address,
    decimals,
    slot_number,
    address,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance_precise,
    pre_balance,
    post_hex_balance,
    post_raw_balance,
    post_balance_precise,
    post_balance,
    net_raw_balance,
    net_balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'contract_address', 'address']) }} AS fact_balances_erc20_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
