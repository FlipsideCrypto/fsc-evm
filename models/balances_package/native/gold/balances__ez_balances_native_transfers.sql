{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'tx_position', 'address'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','phase_4','heal']
) }}

WITH native_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        trace_index,
        trace_address,
        from_address,
        to_address,
        18 AS decimals,
        value * POW(10, decimals) AS raw_amount,
        value AS amount,
        tx_succeeded,
        trace_succeeded
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        value > 0
        AND type NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )
        AND from_address <> to_address

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
tx_fees AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        from_address,
        to_address,
        18 AS decimals,
        tx_fee * POW(10, decimals) AS raw_tx_fee,
        tx_fee,
        tx_succeeded
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_fee > 0
        AND from_address <> '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND modified_timestamp > (
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
        trace_index,
        trace_address,
        to_address AS address,
        raw_amount,
        amount,
        decimals,
        tx_succeeded,
        trace_succeeded,
        'credit' AS direction
    FROM
        native_transfers
    WHERE
        to_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        trace_index,
        trace_address,
        from_address AS address,
        raw_amount * -1 AS raw_amount,
        amount * -1 AS amount,
        decimals,
        tx_succeeded,
        trace_succeeded,
        'debit' AS direction
    FROM
        native_transfers
    WHERE
        from_address <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        -1 AS trace_index, --ensure fees process prior to ORIGIN trace
        'FEE' AS trace_address,
        from_address AS address,
        raw_tx_fee * -1 AS raw_amount,
        tx_fee * -1 AS amount,
        decimals,
        tx_succeeded,
        TRUE AS trace_succeeded,
        'debit' AS direction
    FROM
        tx_fees
),
running_balances AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        trace_index,
        trace_address,
        address,
        decimals,
        tx_succeeded,
        trace_succeeded,
        direction,
        SUM(raw_amount) OVER (
            PARTITION BY address
            ORDER BY block_number, tx_position, trace_index
            ROWS UNBOUNDED PRECEDING
        ) AS balance_raw,
        SUM(amount) OVER (
                PARTITION BY address
                ORDER BY block_number, tx_position, trace_index
                ROWS UNBOUNDED PRECEDING
            ) AS balance
    FROM
        all_transfers
)
SELECT
    block_number,
    block_timestamp,
    tx_position,
    tx_hash,
    address,
    decimals,
    tx_succeeded,
    trace_succeeded,
    balance_raw,
    balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'address']) }} AS ez_balances_native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    running_balances
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY block_number, tx_position, address 
    ORDER BY trace_index DESC
) = 1
