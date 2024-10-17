{# Set variables #}
{%- set source_name = 'RECEIPTS_BY_HASH' -%}
{%- set model_type = 'COMPLETE' -%}

{%- set full_refresh_type = var((source_name ~ '_complete_full_refresh').upper(), false) -%}

{%- set unique_key = 'complete_' ~ source_name.lower() ~ '_id' -%}

{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number, tx_hash)"%}

{# Log configuration details #}
{{ log_complete_details(
    post_hook = post_hook,
    full_refresh_type = full_refresh_type,
    uses_receipts_by_hash = uses_receipts_by_hash
) }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze__' ~ source_name.lower()) }}

{{ config (
    materialized = "incremental",
    unique_key = unique_key,
    cluster_by = "ROUND(block_number, -3)",
    post_hook = post_hook,
    full_refresh = full_refresh_type,
    tags = ['streamline_core_' ~ model_type.lower(), 'receipts_by_hash']
) }}

{# Main query starts here #}
SELECT
    tx_hash,
    block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_hash']) }} AS complete_{{ source_name.lower() }}_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__' ~ source_name.lower()) }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__' ~ source_name.lower() ~ '_fr') }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number desc, _inserted_timestamp DESC)) = 1
