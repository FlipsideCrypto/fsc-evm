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

WITH erc20_transfers AS (
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
        slot_number,
        tx_succeeded
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver__balance_slots') }} v --limits balances to verified assets only
        USING (contract_address)
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND DATA IS NOT NULL
        AND raw_amount IS NOT NULL
        AND slot_number IS NOT NULL
        AND num_slots = 1 --only include contracts with a single balanceOf slot

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
        slot_number,
        tx_succeeded
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver__balance_slots') }} v 
        USING (contract_address)
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
        IFF(p.decimals IS NULL AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', 18, p.decimals) AS decimals,
        p.symbol,
        address,
        storage_key,
        slot_number,
        pre_storage_hex AS pre_balance_hex,
        utils.udf_hex_to_int(pre_storage_hex) AS pre_balance_raw,
        IFF(decimals IS NULL, NULL,utils.udf_decimal_adjust(
            pre_balance_raw,
            decimals
        )) AS pre_balance_precise,
        pre_balance_precise :: FLOAT AS pre_balance,
        IFF(decimals IS NULL, NULL, ROUND(
            pre_balance * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS pre_balance_usd,
        post_storage_hex AS post_balance_hex,
        utils.udf_hex_to_int(post_storage_hex) AS post_balance_raw,
        IFF(decimals IS NULL, NULL,utils.udf_decimal_adjust(
            post_balance_raw,
            decimals
        )) AS post_balance_precise,
        post_balance_precise :: FLOAT AS post_balance,
        IFF(decimals IS NULL, NULL, ROUND(
            post_balance * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS post_balance_usd,
        TRY_TO_NUMBER(post_balance_raw) - TRY_TO_NUMBER(pre_balance_raw) AS net_balance_raw,
        post_balance_precise - pre_balance_precise AS net_balance,
        transfer_amount,
        tx_succeeded
    FROM
        state_storage s
        INNER JOIN transfer_mapping t USING (
            block_number,
            tx_position,
            contract_address,
            storage_key
        )
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} 
        p
        ON s.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        AND p.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p1.HOUR
        AND p1.is_native
    WHERE
        net_balance_raw = transfer_amount
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
        IFF(p.decimals IS NULL AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', 18, p.decimals) AS decimals_heal,
        p.symbol AS symbol_heal,
        slot_number,
        address,
        pre_balance_hex,
        pre_balance_raw,
        IFF(decimals_heal IS NULL, NULL,utils.udf_decimal_adjust(
            pre_balance_raw,
            decimals_heal
        )) AS pre_balance_precise_heal,
        pre_balance_precise_heal :: FLOAT AS pre_balance_heal,
        IFF(decimals_heal IS NULL, NULL, ROUND(
            pre_balance_heal * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS pre_balance_usd_heal,
        post_balance_hex,
        post_balance_raw,
        IFF(decimals_heal IS NULL, NULL,utils.udf_decimal_adjust(
            post_balance_raw,
            decimals_heal
        )) AS post_balance_precise_heal,
        post_balance_precise_heal :: FLOAT AS post_balance_heal,
        IFF(decimals_heal IS NULL, NULL, ROUND(
            post_balance_heal * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS post_balance_usd_heal,
        net_balance_raw,
        net_balance
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core__fact_blocks') }}
        b USING(block_number)
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} 
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p.hour
        AND p.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p1.HOUR
        AND p1.is_native
    WHERE
        (t.block_timestamp IS NULL
        OR t.pre_balance_usd IS NULL
        OR t.post_balance_usd IS NULL)
        AND (
            p.price IS NOT NULL 
            OR (contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}' AND p1.price IS NOT NULL)
        )
        
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
        symbol,
        slot_number,
        address,
        pre_balance_hex,
        pre_balance_raw,
        pre_balance_precise,
        pre_balance,
        pre_balance_usd,
        post_balance_hex,
        post_balance_raw,
        post_balance_precise,
        post_balance,
        post_balance_usd,
        net_balance_raw,
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
    decimals_heal AS decimals,
    symbol_heal AS symbol,
    slot_number,
    address,
    pre_balance_hex,
    pre_balance_raw,
    pre_balance_precise_heal AS pre_balance_precise,
    pre_balance_heal AS pre_balance,
    pre_balance_usd_heal AS pre_balance_usd,
    post_balance_hex,
    post_balance_raw,
    post_balance_precise_heal AS post_balance_precise,
    post_balance_heal AS post_balance,
    post_balance_usd_heal AS post_balance_usd,
    net_balance_raw,
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
    symbol,
    slot_number,
    address,
    pre_balance_hex,
    pre_balance_raw,
    pre_balance_precise,
    pre_balance,
    pre_balance_usd,
    post_balance_hex,
    post_balance_raw,
    post_balance_precise,
    post_balance,
    post_balance_usd,
    net_balance_raw,
    net_balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'contract_address', 'address']) }} AS ez_balances_erc20_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
