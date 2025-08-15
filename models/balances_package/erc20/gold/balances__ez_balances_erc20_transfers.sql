{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'address', 'contract_address'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    post_hook = '{{ unverify_balances() }}',
    tags = ['gold','balances','phase_4','heal']
) }}

WITH verified_assets AS (

    SELECT
        token_address AS contract_address
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
        AND asset_id IS NOT NULL
        AND token_address IS NOT NULL
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
        AND raw_amount_precise IS NOT NULL

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
        utils.udf_hex_to_int(DATA) AS raw_amount_precise,
        TRY_TO_NUMBER(raw_amount_precise) AS raw_amount,
        18 AS decimals,
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
        AND raw_amount_precise IS NOT NULL

{% if is_incremental() %}
AND l.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        to_address AS address,
        raw_amount_precise,
        raw_amount,
        decimals,
        tx_succeeded,
        'credit' AS direction
    FROM
        erc20_transfers
    WHERE
        to_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        from_address AS address,
        raw_amount_precise * -1 AS raw_amount_precise,
        raw_amount * -1 AS raw_amount,
        decimals,
        tx_succeeded,
        'debit' AS direction
    FROM
        erc20_transfers
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        to_address AS address,
        raw_amount_precise,
        raw_amount,
        decimals,
        tx_succeeded,
        'credit' AS direction
    FROM
        wrapped_native_transfers
    WHERE
        to_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        from_address AS address,
        raw_amount_precise * -1 AS raw_amount_precise,
        raw_amount * -1 AS raw_amount,
        decimals,
        tx_succeeded,
        'debit' AS direction
    FROM
        wrapped_native_transfers
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'
),
running_balances AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        event_index,
        contract_address,
        address,
        decimals,
        tx_succeeded,
        direction,
        SUM(raw_amount_precise) OVER (
            PARTITION BY contract_address, address
            ORDER BY block_number, tx_position, event_index
            ROWS UNBOUNDED PRECEDING
        ) AS balance_precise_raw,
        SUM(raw_amount) OVER (
            PARTITION BY contract_address, address
            ORDER BY block_number, tx_position, event_index
            ROWS UNBOUNDED PRECEDING
        ) AS balance_raw,
        CASE
            WHEN decimals IS NOT NULL THEN SUM(raw_amount) OVER (
                PARTITION BY contract_address, address
                ORDER BY block_number, tx_position, event_index
                ROWS UNBOUNDED PRECEDING
            ) / POW(10, decimals)
            ELSE NULL
        END AS balance
    FROM
        all_transfers
)
SELECT
    block_number,
    block_timestamp,
    contract_address,
    address,
    decimals,
    tx_succeeded,
    balance_precise_raw,
    balance_raw,
    balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'address', 'contract_address']) }} AS ez_balances_erc20_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    running_balances
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY block_number, contract_address, address 
    ORDER BY tx_position DESC, event_index DESC
) = 1
