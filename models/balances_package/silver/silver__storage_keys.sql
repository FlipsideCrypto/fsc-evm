{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['storage_keys_id'],
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
        contract_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        TRY_TO_NUMBER(raw_amount_precise) AS raw_amount,
        slot_number,
        tx_succeeded,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver__balance_slots') }}
        v --limits balances to verified assets only
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
        tx_succeeded,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver__balance_slots') }}
        v USING (contract_address)
    WHERE
        topic_0 IN (
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            -- withdraw
            '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' --deposit
        )
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
all_addresses AS (
    SELECT
        DISTINCT from_address AS address,
        slot_number,
        block_number,
        modified_timestamp
    FROM
        erc20_transfers
    UNION
    SELECT
        DISTINCT to_address AS address,
        slot_number,
        block_number,
        modified_timestamp
    FROM
        erc20_transfers
    UNION
    SELECT
        DISTINCT from_address AS address,
        slot_number,
        block_number,
        modified_timestamp
    FROM
        wrapped_native_transfers
    UNION
    SELECT
        DISTINCT to_address AS address,
        slot_number,
        block_number,
        modified_timestamp
    FROM
        wrapped_native_transfers
)
SELECT
    address,
    slot_number,
    utils.udf_mapping_slot(
        address,
        slot_number
    ) AS storage_key,
    {{ dbt_utils.generate_surrogate_key(['address', 'slot_number']) }} AS storage_keys_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_addresses qualify ROW_NUMBER() over (
        PARTITION BY address,
        slot_number
        ORDER BY
            block_number DESC,
            modified_timestamp DESC
    ) = 1
