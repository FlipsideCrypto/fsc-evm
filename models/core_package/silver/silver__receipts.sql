{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

-- depends_on: {{ ref('bronze__receipts') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    full_refresh = false,
    tags = ['core','silver']
) }}

{% if uses_receipts_by_hash %}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key,
        tx_hash,
        DATA:result as receipts_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__receipts_by_hash') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA:result is not null
    {% else %}
    {{ ref('bronze__receipts_by_hash_fr') }}
    WHERE DATA:result is not null
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
QUALIFY ROW_NUMBER() OVER (PARTITION BY receipts_id ORDER BY _inserted_timestamp DESC) = 1

{% endif %}