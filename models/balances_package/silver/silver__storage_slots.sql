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
        tx_hash,
        event_index,
        from_address,
        to_address,
        contract_address,
        raw_amount
    FROM
        {{ ref('core__ez_token_transfers') }}
        {% if is_incremental() %}
        WHERE
            contract_address NOT IN (
                SELECT
                    contract_address
                FROM
                    {{ this }})
        {% endif %}
        --temp filter for testing
        AND tx_hash IN (
            '0x35db4db6fe1abeff07c21bb0d76eb15d5dfb348911c6d4602f8a08a288054ebc',
            '0xdf1720de30d55609dfafd9642327bede3f1f5da8c03176e1ddfab3c5b3d973a3',
            '0x35db4db6fe1abeff07c21bb0d76eb15d5dfb348911c6d4602f8a08a288054ebc' --simple usdc transfers
            )
        qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        block_number DESC)) = 1
),
transfer_direction AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        to_address AS user_address,
        contract_address,
        raw_amount
    FROM
        erc20_transfers
    UNION ALL
    SELECT
        block_number,
        tx_hash,
        event_index,
        from_address AS user_address,
        contract_address,
        (
            -1 * raw_amount
        ) AS raw_amount
    FROM
        erc20_transfers
),
direction_agg AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        user_address,
        contract_address,
        SUM(raw_amount) AS net_amount
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
            tx_position,
            tx_hash,
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
    ),
    transfer_mapping AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            event_index,
            contract_address,
            user_address,
            utils.udf_mapping_slot(
                user_address,
                rn
            ) AS storage_key,
            rn AS slot_number,
            net_amount AS transfer_amount
        FROM
            direction_agg,
            num_generator
    ),
    slot_finder AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            event_index,
            contract_address,
            user_address,
            storage_key,
            slot_number,
            pre_storage_hex,
            post_storage_hex,
            utils.udf_hex_to_int(pre_storage_hex) AS pre_storage_value,
            utils.udf_hex_to_int(post_storage_hex) AS post_storage_value,
            post_storage_value - pre_storage_value AS net_amount,
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
            net_amount = transfer_amount
    )
        SELECT
            contract_address,
            ARRAY_AGG(
                DISTINCT slot_number
            ) AS slot_number_array
        FROM
            slot_finder
        GROUP BY
            ALL

