{%- if var('GLOBAL_USES_V2_FSC_EVM', False) -%}

-- depends_on: {{ ref('bronze__transactions') }}

{% set silver_full_refresh = var('SILVER_FULL_REFRESH', false) %}

{% if not silver_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = silver_full_refresh,
    tags = ['core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['core']
) }}

{% endif %}

WITH bronze_transactions AS (
    SELECT 
        block_number,
        partition_key,
        VALUE :array_index :: INT AS tx_position,
        DATA as transaction_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze__transactions') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA is not null
    {% else %}
    {{ ref('bronze__transactions_fr') }}
    WHERE DATA is not null
    {% endif %}
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
QUALIFY ROW_NUMBER() OVER (PARTITION BY transactions_id ORDER BY _inserted_timestamp DESC) = 1
{%- endif -%}