{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, symbol, name), SUBSTRING(address, symbol, name)",
    tags = ['gold_core','phase_2']
) }}

WITH created_contracts AS (

    SELECT
        created_contract_address AS address,
        block_number AS created_block_number,
        block_timestamp AS created_block_timestamp,
        tx_hash AS created_tx_hash,
        creator_address AS creator_address,
        created_contracts_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__created_contracts') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
    ),
    genesis_contracts AS (
        SELECT
            contract_address AS address,
            0 AS created_block_number,
            '1970-01-01 00:00:00' AS created_block_timestamp,
            'GENESIS' AS created_tx_hash,
            'GENESIS' AS creator_address,
            {{ dbt_utils.generate_surrogate_key(
                ['contract_address']
            ) }} AS created_contracts_id,
            max_inserted_timestamp_logs AS inserted_timestamp,
            max_inserted_timestamp_logs AS modified_timestamp
        FROM
            {{ ref('silver__relevant_contracts') }}
            r
        WHERE
            total_event_count > 0
            AND NOT EXISTS (
                SELECT
                    1
                FROM
                    {{ ref('silver__created_contracts') }} C
                WHERE
                    r.contract_address = C.created_contract_address
            )

{% if is_incremental() %}
AND max_inserted_timestamp_logs > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }})
    {% endif %}
),
combined AS (
    SELECT
        *
    FROM
        created_contracts
    UNION ALL
    SELECT
        *
    FROM
        genesis_contracts
)
SELECT
    address,
    c1.token_symbol AS symbol,
    c1.token_name AS NAME,
    c1.token_decimals AS decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    COALESCE (
        c0.created_contracts_id,
        {{ dbt_utils.generate_surrogate_key(
            ['c0.address']
        ) }}
    ) AS dim_contracts_id,
    GREATEST(COALESCE(c0.inserted_timestamp, '2000-01-01'), COALESCE(c1.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    combined c0
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON LOWER(
        c0.address
    ) = LOWER(c1.contract_address)

{% if is_incremental() %}
WHERE
    c0.modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
            OR c1.modified_timestamp > (
                SELECT
                    COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
                FROM
                    {{ this }})
                {% endif %}
