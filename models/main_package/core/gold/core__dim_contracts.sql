{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, symbol, name)",
    tags = ['gold','core','phase_2']
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
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
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
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
    FROM
        {{ this }})
    {% endif %}
),
combined AS (
    SELECT
        address,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        created_contracts_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        created_contracts
    UNION ALL
    SELECT
        address,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        created_contracts_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        genesis_contracts
),
new_contracts AS (
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
        GREATEST(COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp,
        1 AS priority
    FROM
        combined c0
        LEFT JOIN {{ ref('silver__contracts') }}
        c1
        ON LOWER(
            c0.address
        ) = LOWER(
            c1.contract_address
        )
)

{% if is_incremental() %},
metadata_heal AS (
    SELECT
        address,
        C.token_symbol AS symbol,
        C.token_name AS NAME,
        C.token_decimals AS decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        dim_contracts_id,
        GREATEST(COALESCE(t.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
        GREATEST(COALESCE(t.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
        2 AS priority
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__contracts') }} C
        ON LOWER(
            t.address
        ) = LOWER(
            C.contract_address
        )
    WHERE
        C.inserted_timestamp > (
            SELECT
                MAX(inserted_timestamp)
            FROM
                {{ this }}
        )
        AND (
            t.symbol IS NULL
            OR t.name IS NULL
            OR t.decimals IS NULL
        )
)
{% endif %},
FINAL AS (
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        created_block_number,
        created_block_timestamp,
        created_tx_hash,
        creator_address,
        dim_contracts_id,
        inserted_timestamp,
        modified_timestamp,
        priority
    FROM
        new_contracts

{% if is_incremental() %}
UNION ALL
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    dim_contracts_id,
    inserted_timestamp,
    modified_timestamp,
    priority
FROM
    metadata_heal
{% endif %}
)
SELECT
    address,
    symbol,
    NAME,
    decimals,
    created_block_number,
    created_block_timestamp,
    created_tx_hash,
    creator_address,
    dim_contracts_id,
    inserted_timestamp,
    modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY address
        ORDER BY
            priority ASC,
            modified_timestamp DESC
    ) = 1
