{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% set silver_full_refresh = var('SILVER_FULL_REFRESH', false) %}
{% set unique_key = "tx_hash" if uses_receipts_by_hash else "block_number" %}
{% set source_name = 'RECEIPTS_BY_HASH' if uses_receipts_by_hash else 'RECEIPTS' %}

-- depends_on: {{ ref('bronze__' ~ source_name.lower()) }}

{% if not silver_full_refresh %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = silver_full_refresh,
    tags = ['silver_core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['silver_core']
) }}

{% endif %}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key,
        {% if uses_receipts_by_hash %}
            tx_hash,
            DATA:result AS receipts_json,
        {% else %}
            receipts_json :transactionHash :: STRING AS tx_hash,
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
    {% if uses_receipts_by_hash %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% else %}
    {{ ref('bronze__' ~ source_name.lower() ~ '_fr') }}
    WHERE 
    {% if uses_receipts_by_hash %}
        DATA:result IS NOT NULL
    {% else %}
        DATA IS NOT NULL
    {% endif %}
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    tx_hash,
    receipts_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number DESC, _inserted_timestamp DESC) = 1