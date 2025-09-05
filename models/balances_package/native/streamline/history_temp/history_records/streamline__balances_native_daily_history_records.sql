{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "incremental",
    unique_key = "balances_native_daily_history_records_id",
    cluster_by = "ROUND(block_number, -3)",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','balances','history','native','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_number >= {{ vars.BALANCES_SL_START_BLOCK }}
),
traces AS (
    SELECT
        block_number,
        block_timestamp,
        from_address AS address1,
        to_address AS address2
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        value > 0
        AND type NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )
        AND block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        )
        AND block_number >= {{ vars.BALANCES_SL_START_BLOCK }} 
        --only include traces prior to 1 day ago
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '72 hours'
            FROM {{ this }}
        )
    {% endif %}
),
tx_fees AS (
    SELECT
        block_number,
        block_timestamp,
        from_address AS address
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_fee > 0
        AND from_address <> '0x0000000000000000000000000000000000000000'
        AND block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        AND block_number >= {{ vars.BALANCES_SL_START_BLOCK }} 
        --only include txns prior to 1 day ago
        {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT MAX(modified_timestamp) - INTERVAL '72 hours'
                FROM {{ this }}
            )
        {% endif %}
),
native_transfers AS (
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address1 AS address
    FROM
        traces
    WHERE
        address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address2 AS address
    FROM
        traces
    WHERE
        address2 <> '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address
    FROM
        tx_fees
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.address
    FROM
        native_transfers t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
        --max daily block_number during the selected period, for each address
    WHERE
        t.block_date IS NOT NULL
        AND d.block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        )
)
SELECT
    block_number,
    block_date,
    address,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address']) }} AS balances_native_daily_history_records_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    to_do qualify (ROW_NUMBER() over (PARTITION BY balances_native_daily_history_records_id
ORDER BY
    modified_timestamp DESC)) = 1