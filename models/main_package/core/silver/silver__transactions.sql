-- depends_on: {{ ref('bronze__transactions') }}

{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    -- post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)", -- Moved to daily_search_optimization maintenance model
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','core','phase_2']
) }}

WITH bronze_transactions AS (
    SELECT 
        block_number,
        partition_key,
        COALESCE(
            VALUE :array_index :: INT,
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    VALUE :data :transactionIndex :: STRING
                )
            )
        ) AS tx_position,
        DATA AS transaction_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__transactions') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA IS NOT NULL
    {% else %}
    {{ ref('bronze__transactions_fr') }}
    WHERE DATA IS NOT NULL
    {% endif %}
    AND block_number >= {{ vars.GLOBAL_START_BLOCK }}
)

SELECT 
    block_number,
    partition_key,
    tx_position,
    transaction_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_position']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_transactions
WHERE tx_position IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY transactions_id ORDER BY _inserted_timestamp DESC) = 1