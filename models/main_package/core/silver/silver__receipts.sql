-- depends_on: {{ ref('bronze__' ~ MAIN_CORE_RECEIPTS_SOURCE_NAME.lower()) }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['silver_core']
) }}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key,
        {% if USES_RECEIPTS_BY_HASH %}
            tx_hash,
            DATA:result AS receipts_json,
        {% else %}
            array_index,
            DATA AS receipts_json,
        {% endif %}
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__' ~ source_name.lower()) }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND 
    {% if USES_RECEIPTS_BY_HASH %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% else %}
    {{ ref('bronze__' ~ source_name.lower() ~ '_fr') }}
    WHERE 
    {% if USES_RECEIPTS_BY_HASH %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    {% if USES_RECEIPTS_BY_HASH %}
        tx_hash,
    {% else %}
        array_index,
    {% endif %}
    receipts_json,
    _inserted_timestamp,
    {% if USES_RECEIPTS_BY_HASH %}
        {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS receipts_id,
    {% else %}
        {{ dbt_utils.generate_surrogate_key(['block_number','array_index']) }} AS receipts_id,
    {% endif %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
{% if USES_RECEIPTS_BY_HASH %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number DESC, _inserted_timestamp DESC) = 1
{% else %}
QUALIFY(ROW_NUMBER() OVER (PARTITION BY block_number, array_index ORDER BY _inserted_timestamp DESC)) = 1
{% endif %} 