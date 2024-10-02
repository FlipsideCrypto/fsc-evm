{% macro streamline_core_complete() %}

{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- if '__' in this.identifier -%}
    {%- set model_parts = identifier_parts[1].split('_') -%}
    {%- set model_type = model_parts[-1] -%}
    {%- set model = '_'.join(model_parts[:-1]) -%}
{%- else -%}
    {%- set model_parts = this.identifier.split('_') -%}
    {%- set model_type = model_parts[-1] -%}
    {%- set model = '_'.join(model_parts[:-1]) -%}
{%- endif -%}

{# Set full refresh type based on model configuration #}
{%- set full_refresh_type = var(('complete_' ~ model ~ '_full_refresh').upper(), False) -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("Model: " ~ model, info=True) }}
    {{ log("Model Type: Complete", info=True) }}
    {{ log("Full Refresh Type: " ~ full_refresh_type, info=True) }}
    {{ log("Materialization: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

-- depends_on: {{ ref('bronze__streamline_' ~ model) }}

{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    full_refresh = full_refresh_type,
    tags = ['streamline_core_complete']
) }}

SELECT
    block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS complete_{{ model }}_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__streamline_' ~ model) }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__streamline_fr_' ~ model) }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY block_number ORDER BY _inserted_timestamp DESC)) = 1

{% endmacro %}
