{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','phase_4']
) }}

WITH erc20_transfers AS (

    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address,
        raw_amount,
        decimals
    FROM
        {{ ref('core__ez_token_transfers') }}
        WHERE 1=1
        {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }})
        {% endif %}
        --temp filter for testing
        AND block_number IN (25804285,25804301,25804312,25804315)
),
transfer_direction AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
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
        event_index,
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
        event_index,
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
        post_state_json
    FROM
        {{ ref('silver__state_tracer') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% endif %}
    ),
    pre_state AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            pre_state_json,
            f.key AS address,
            f.value :storage AS pre_storage
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => pre_state_json
            ) f
        WHERE
            f.value :storage IS NOT NULL
    ),
    pre_state_storage AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            pre_state_json,
            address,
            pre_storage,
            f.key :: STRING AS storage_key,
            f.value :: STRING AS pre_storage_value_hex
        FROM
            pre_state,
            LATERAL FLATTEN(
                input => pre_storage
            ) f
    ),
    post_state AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            post_state_json,
            f.key AS address,
            f.value :storage AS post_storage
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => post_state_json
            ) f
        WHERE
            f.value :storage IS NOT NULL
    ),
    post_state_storage AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            post_state_json,
            address,
            post_storage,
            f.key :: STRING AS storage_key,
            f.value :: STRING AS post_storage_value_hex
        FROM
            post_state,
            LATERAL FLATTEN(
                input => post_storage
            ) f
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
            TABLE(GENERATOR(rowcount => 51)) 
            {# what are the max slots for erc20? potentially reduce this? #}
    ),
    transfer_mapping AS (
        SELECT
            block_number,
            block_timestamp,
            tx_position,
            tx_hash,
            event_index,
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
    final AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        address,
        storage_key,
        slot_number,
        pre_storage_hex AS pre_hex_balance,
        utils.udf_hex_to_int(pre_storage_hex) AS pre_raw_balance,
        utils.udf_decimal_adjust(
            pre_raw_balance,
            decimals
        ) AS pre_state_balance,
        post_storage_hex AS post_hex_balance,
        utils.udf_hex_to_int(post_storage_hex) AS post_raw_balance,
        utils.udf_decimal_adjust(
            post_raw_balance,
            decimals
        ) AS post_state_balance,
        post_raw_balance - pre_raw_balance AS net_raw_balance,
        post_state_balance - pre_state_balance AS net_state_balance,
        transfer_amount
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
    SELECT 
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        address,
        pre_hex_balance,
        pre_raw_balance,
        pre_state_balance,
        post_hex_balance,
        post_raw_balance,
        post_state_balance,
        net_raw_balance,
        net_state_balance,
        {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'contract_address', 'address']) }} AS fact_balances_erc20_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM final
    {# + add heal logic for block_timestamp and decimals #}


{# 
    Add test to verify slots:
    if > 1 slots in array or NULL, then false positive or missing slot. 
    Flag and handle separately (e.g. rebase tokens) 

    SELECT
        contract_address,
        ARRAY_AGG(
            DISTINCT slot_number
        ) AS slot_number_array
    FROM
        this model
    GROUP BY contract_address 
#}

