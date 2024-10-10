{%- if var('GLOBAL_ENABLE_FSC_EVM', False) -%}
{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% set silver_full_refresh = var('SILVER_FULL_REFRESH', false) %}

{% if uses_receipts_by_hash %}

-- depends_on: {{ ref('bronze__receipts_by_hash') }}

{% if not silver_full_refresh %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "tx_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = silver_full_refresh,
    tags = ['core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "tx_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['core']
) }}

{% endif %}

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
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number desc, _inserted_timestamp DESC) = 1

{% endif %}
{%- endif -%}