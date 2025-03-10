{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze__transactions') }}

{% if vars.GLOBAL_STREAMLINE_FR_ENABLED %}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_core_complete']
) }}
{% else %}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline_core_complete']
) }}
{% endif %}

{# Main query starts here #}
SELECT
    block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__transactions') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__transactions_fr') }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY block_number ORDER BY _inserted_timestamp DESC)) = 1