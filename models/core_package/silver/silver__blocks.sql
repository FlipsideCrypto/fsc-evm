-- depends_on: {{ ref('bronze__blocks') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    full_refresh = false,
    tags = ['core','silver']
) }}


WITH bronze_blocks as (
    SELECT 
        block_number,
        partition_key,
        DATA:result as block_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__blocks') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    )
    {% else %}
    {{ ref('bronze__blocks_fr') }}
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    block_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_blocks
QUALIFY ROW_NUMBER() OVER (PARTITION BY blocks_id ORDER BY _inserted_timestamp DESC) = 1