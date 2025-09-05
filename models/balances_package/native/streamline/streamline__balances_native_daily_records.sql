{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "incremental",
    unique_key = "balances_native_daily_records_id",
    cluster_by = "ROUND(block_number, -3)",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['balances','records','native','phase_4']
) }}

WITH traces AS (
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
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '72 hours'
            FROM {{ this }}
        )
    {% endif %}
),
native_transfers_snapshot AS (
    SELECT
        DISTINCT 
        ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE AS block_date,
        address1 AS address
    FROM
        traces
    WHERE
        address1 <> '0x0000000000000000000000000000000000000000'
        AND block_timestamp :: DATE <= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    UNION
    SELECT
        DISTINCT 
        ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE AS block_date,
        address2 AS address
    FROM
        traces
    WHERE
        address2 <> '0x0000000000000000000000000000000000000000'
        AND block_timestamp :: DATE <= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    UNION
    SELECT
        DISTINCT 
        ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE AS block_date,
        address
    FROM
        tx_fees
    WHERE
        block_timestamp :: DATE <= ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
),
native_transfers_history AS (
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address1 AS address
    FROM
        traces
    WHERE
        address1 <> '0x0000000000000000000000000000000000000000'
        AND block_date > ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address2 AS address
    FROM
        traces
    WHERE
        address2 <> '0x0000000000000000000000000000000000000000'
        AND block_date > ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        address
    FROM
        tx_fees
    WHERE
        block_date > ('{{ vars.BALANCES_SL_START_DATE }}' :: TIMESTAMP) :: DATE
),
all_transfers AS (
    SELECT * FROM native_transfers_snapshot
    UNION
    SELECT * FROM native_transfers_history
)
SELECT
    block_date,
    address,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address']) }} AS balances_native_daily_records_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_transfers qualify (ROW_NUMBER() over (PARTITION BY balances_native_daily_records_id
ORDER BY
    modified_timestamp DESC)) = 1