-- depends_on: {{ ref('bronze__confirm_blocks') }}

{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{% if is_incremental() %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','core','confirm_blocks','phase_2']
) }}
{% else %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','core','confirm_blocks','phase_2']
) }}
{% endif %}

--temp logic for migration
{% if vars.GLOBAL_SILVER_FR_ENABLED is none and var('TEMP_CONFIRM_BLOCKS_FR_ENABLED', false) %}

SELECT
    block_number,
    ROUND(block_number,-3) AS partition_key,
    NULL :: VARIANT AS block_json,
    block_hash,
    tx_hash,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS confirm_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'logs_temp',
        'confirmed_blocks'
    ) }}

{% else %}

WITH bronze_confirm_blocks AS (
    SELECT 
        block_number,
        partition_key,
        DATA:result AS block_json,
        block_json :hash :: STRING AS block_hash,
        block_json :transactions AS txs,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
        {% if vars.MAIN_CORE_SILVER_CONFIRM_BLOCKS_FULL_RELOAD_ENABLED %}
            {{ ref('bronze__confirm_blocks') }}
            WHERE block_number >= (
                SELECT COALESCE(MAX(block_number), 0) FROM {{ this }}
            )
            AND block_number < (
                SELECT COALESCE(MAX(block_number), 0) FROM {{ this }} 
            ) +5000000
            AND DATA:result IS NOT NULL
        {% else %}
            {{ ref('bronze__confirm_blocks') }}
            WHERE _inserted_timestamp >= (
                SELECT COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
                FROM {{ this }}
            ) AND DATA:result IS NOT NULL
        {% endif %}
    {% else %}
        {{ ref('bronze__confirm_blocks_fr') }}
        WHERE DATA:result IS NOT NULL
    {% endif %}
    qualify(ROW_NUMBER() over (PARTITION BY block_number ORDER BY _inserted_timestamp DESC)) = 1
)

SELECT 
    block_number,
    partition_key,
    block_json,
    block_hash,
    VALUE :: STRING AS tx_hash,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS confirm_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_confirm_blocks,
    LATERAL FLATTEN (
        input => txs
    )
{% endif %}
