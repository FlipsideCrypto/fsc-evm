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
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND block_timestamp >= '2025-07-07 09:30:00.000' :: TIMESTAMP 

{% if is_incremental() %}
AND
    modified_timestamp > (
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
        event_index,
        contract_address,
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
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topic_0 IN (
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            -- withdraw
            '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c' --deposit
        )
        AND block_timestamp >= '2025-07-07 09:30:00.000' :: TIMESTAMP 

{% if is_incremental() %}
AND
    modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

all_addresses AS (
    SELECT DISTINCT from_address AS address FROM erc20_transfers
    UNION
    SELECT DISTINCT to_address AS address FROM erc20_transfers
    UNION
    SELECT DISTINCT from_address AS address FROM wrapped_native_transfers
    UNION
    SELECT DISTINCT to_address AS address FROM wrapped_native_transfers
),

final_storage_keys AS (
    SELECT 
        a.address,
        v.slot_number,
        utils.udf_mapping_slot(a.address, v.slot_number) AS storage_key
    FROM all_addresses a
    CROSS JOIN {{ ref('silver__balance_slots') }} v
    WHERE a.address IS NOT NULL 
    AND v.num_slots = 1
    AND v.slot_number IS NOT NULL
)

SELECT
    address,
    slot_number,
    storage_key,
    {{ dbt_utils.generate_surrogate_key(['address', 'slot_number']) }} AS storage_keys_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM final_storage_keys qualify (ROW_NUMBER() over (
        PARTITION BY address,
        slot_number
        ORDER BY
            address
    )) = 1