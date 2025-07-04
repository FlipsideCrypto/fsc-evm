{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__' ~ vars.MAIN_CORE_SILVER_RECEIPTS_SOURCE_NAME.lower()) }}
-- depends_on: {{ ref('bronze__' ~ vars.MAIN_CORE_SILVER_RECEIPTS_SOURCE_NAME.lower() ~ '_fr') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_SILVER_RECEIPTS_UNIQUE_KEY,
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = vars.MAIN_CORE_SILVER_RECEIPTS_POST_HOOK,
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','core','phase_2']
) }}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key,
        {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
            tx_hash,
            DATA:result AS receipts_json,
        {% else %}
            array_index,
            DATA AS receipts_json,
        {% endif %}
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__' ~ vars.MAIN_CORE_SILVER_RECEIPTS_SOURCE_NAME.lower()) }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND 
    {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% else %}
    {{ ref('bronze__' ~ vars.MAIN_CORE_SILVER_RECEIPTS_SOURCE_NAME.lower() ~ '_fr') }}
    WHERE 
    {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
        tx_hash,
    {% else %}
        array_index,
    {% endif %}
    receipts_json,
    _inserted_timestamp,
    {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
        {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS receipts_id,
    {% else %}
        {{ dbt_utils.generate_surrogate_key(['block_number','array_index']) }} AS receipts_id,
    {% endif %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
{% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number DESC, _inserted_timestamp DESC) = 1
{% else %}
QUALIFY(ROW_NUMBER() OVER (PARTITION BY block_number, array_index ORDER BY _inserted_timestamp DESC)) = 1
{% endif %} 