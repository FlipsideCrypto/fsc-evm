{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'round(block_number, -3)'],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','phase_4']
) }}

--depends_on: {{ ref('core__fact_blocks') }}

WITH erc20_transfers AS (

    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        from_address,
        to_address,
        contract_address,
        TRY_TO_NUMBER(raw_amount_precise) AS raw_amount,
        t.decimals
    FROM
        {{ ref('core__ez_token_transfers') }}
        t
        INNER JOIN {{ ref('price__ez_asset_metadata') }}
        m --limit balances to verified assets only
        ON t.contract_address = m.token_address
    WHERE
        is_verified
        AND asset_id IS NOT NULL

{% if is_incremental() %}
AND t.modified_timestamp > (
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
        decimals
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
        decimals
    FROM
        erc20_transfers
),
direction_agg AS (
    SELECT
        block_number,
        block_timestamp,
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
        )

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
        pre_state_storage pre
        FULL OUTER JOIN post_state_storage post USING (
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
        TABLE(GENERATOR(rowcount => 26)) {# no theoretical limit on max slots for erc20, 2-15 is common. Can reduce if needed. #}
),
transfer_mapping AS (
    SELECT
        block_number,
        block_timestamp,
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
        ) AS pre_balance,
        post_storage_hex AS post_hex_balance,
        utils.udf_hex_to_int(post_storage_hex) AS post_raw_balance,
        utils.udf_decimal_adjust(
            post_raw_balance,
            decimals
        ) AS post_balance,
        TRY_TO_NUMBER(post_raw_balance) - TRY_TO_NUMBER(pre_raw_balance) AS net_raw_balance,
        post_balance - pre_balance AS net_balance,
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
        net_raw_balance = transfer_amount
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp AS block_timestamp_heal,
        tx_position,
        tx_hash,
        contract_address,
        decimals,
        slot_number,
        address,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance,
        post_hex_balance,
        post_raw_balance,
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
        contract_address,
        decimals,
        slot_number,
        address,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance,
        post_hex_balance,
        post_raw_balance,
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
    contract_address,
    decimals,
    slot_number,
    address,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance,
    post_hex_balance,
    post_raw_balance,
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
    contract_address,
    decimals,
    slot_number,
    address,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance,
    post_hex_balance,
    post_raw_balance,
    post_balance,
    net_raw_balance,
    net_balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'contract_address', 'address']) }} AS fact_balances_erc20_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL