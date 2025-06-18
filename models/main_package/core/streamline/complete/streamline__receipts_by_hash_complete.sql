{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze__receipts_by_hash') }}

{{ config (
    materialized = "incremental",
    unique_key = "complete_receipts_by_hash_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number, tx_hash)",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','core','complete','receipts_by_hash','phase_1']
) }}

{# Main query starts here #}
SELECT
    tx_hash,
    block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_hash']) }} AS complete_receipts_by_hash_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__receipts_by_hash') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__receipts_by_hash_fr') }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number desc, _inserted_timestamp DESC)) = 1
