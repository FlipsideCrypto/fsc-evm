{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

--depends_on: {{ ref('core__fact_transactions') }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'round(block_number, -3)'],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','phase_4']
) }}

WITH state_tracer_realtime AS (

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
        t
        INNER JOIN {{ ref('silver__balance_slots') }}
        v --limits balances to verified assets only
        ON t.address = v.contract_address
    WHERE
        slot_number IS NOT NULL
        AND num_slots = 1 --only include contracts with a single balanceOf slot

{% if is_incremental() %}
AND 
    t.modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
    {% endif %}
),
{% if is_incremental() %}
new_contracts AS (
    SELECT DISTINCT contract_address
    FROM {{ ref('silver__balance_slots') }}
    WHERE contract_address NOT IN (
        SELECT DISTINCT contract_address 
        FROM {{ this }}
    )
    AND slot_number IS NOT NULL
    AND num_slots = 1
),
state_tracer_history AS (

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
        t
        INNER JOIN new_contracts
        v --limits balances to verified assets only
        ON t.address = v.contract_address
),
{% endif %}
state_tracer AS (
    SELECT *
    FROM state_tracer_realtime
{% if is_incremental() %}
    UNION
    SELECT *
    FROM state_tracer_history
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
balances AS (
    SELECT
        s.block_number,
        tx.block_timestamp,
        s.tx_position,
        s.tx_hash,
        tx.tx_succeeded,
        s.contract_address,
        IFF(p.decimals IS NULL AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', 18, p.decimals) AS decimals_adj,
        p.symbol,
        k.address,
        s.storage_key,
        k.slot_number,
        pre_storage_hex AS pre_balance_hex,
        utils.udf_hex_to_int(pre_storage_hex) AS pre_balance_raw,
        IFF(decimals_adj IS NULL, NULL,utils.udf_decimal_adjust(
            pre_balance_raw,
            decimals_adj
        )) AS pre_balance_precise,
        pre_balance_precise :: FLOAT AS pre_balance,
        IFF(decimals_adj IS NULL, NULL, ROUND(
            pre_balance * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS pre_balance_usd,
        post_storage_hex AS post_balance_hex,
        utils.udf_hex_to_int(post_storage_hex) AS post_balance_raw,
        IFF(decimals_adj IS NULL, NULL,utils.udf_decimal_adjust(
            post_balance_raw,
            decimals_adj
        )) AS post_balance_precise,
        post_balance_precise :: FLOAT AS post_balance,
        IFF(decimals_adj IS NULL, NULL, ROUND(
            post_balance * IFF(contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}', COALESCE(p.price, p1.price), p.price)
        , 2)) AS post_balance_usd,
        TRY_TO_NUMBER(post_balance_raw) - TRY_TO_NUMBER(pre_balance_raw) AS net_balance_raw,
        post_balance_precise - pre_balance_precise AS net_balance
    FROM
        state_storage s
        INNER JOIN {{ ref('silver__storage_keys') }} k USING (storage_key) -- get address that the balance applies to
        LEFT JOIN {{ ref('core__fact_transactions')}} tx USING (block_number, tx_position)
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} 
        p
        ON s.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            tx.block_timestamp
        ) = p.hour
        AND p.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
        ON DATE_TRUNC(
            'hour',
            tx.block_timestamp
        ) = p1.HOUR
        AND p1.is_native
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        tx.block_timestamp AS block_timestamp_heal,
        t.tx_position,
        t.tx_hash,
        tx.tx_succeeded AS tx_succeeded_heal,
        t.contract_address,
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
        LEFT JOIN {{ ref('core__fact_transactions') }}
        tx USING(block_number, tx_position)
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} 
        p
        ON t.contract_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            tx.block_timestamp
        ) = p.hour
        AND p.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }} p1
        ON DATE_TRUNC(
            'hour',
            tx.block_timestamp
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
        decimals_adj AS decimals,
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
    tx_succeeded_heal AS tx_succeeded,
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
