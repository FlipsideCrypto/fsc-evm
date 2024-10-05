{# Extract model information from the identifier #}
{%- set identifier_parts = this.identifier.split('__') -%}
{%- if '__' in this.identifier -%}
    {%- set model = identifier_parts[1] -%}
{%- else -%}
    {%- set model = this.identifier -%}
{%- endif -%}

{# Dynamically get the trim suffix for this specific model #}
{% set trim_suffix = var((model ~ 'trim_suffix').upper(), '_complete') %}

{# Trim model name logic and extract model_type #}
{%- if trim_suffix and model.endswith(trim_suffix) -%}
    {%- set trimmed_model = model[:model.rfind(trim_suffix)] -%}
    {%- set model_type = trim_suffix[1:] -%}  {# Remove the leading underscore #}
{%- else -%}
    {%- set trimmed_model = model -%}
    {%- set model_type = 'complete' -%}
{%- endif -%}

{# Set full refresh type based on model configuration #}
{%- set full_refresh_type = var(('complete_' ~ trimmed_model ~ '_full_refresh').upper(), False) -%}

{%- set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)" -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Name Output Details ===", info=True) }}

    {{ log("Original Model: " ~ model, info=True) }}
    {{ log("Trimmed Model: " ~ trimmed_model, info=True) }}
    {{ log("Trim Suffix: " ~ trim_suffix, info=True) }}
    {{ log("Model Type: " ~ model_type, info=True) }}
    {{ log("", info=True) }}


    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = "' ~ config.get('cluster_by') ~ '",\n' %}
    {% set config_log = config_log ~ '    post_hook = "' ~ post_hook ~ '",\n' %}
    {% set config_log = config_log ~ '    full_refresh = ' ~ full_refresh_type ~ ',\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

-- depends_on: {{ ref('bronze__' ~ trimmed_model) }}

{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = post_hook,
    full_refresh = full_refresh_type,
    tags = ['streamline_core_complete']
) }}

SELECT
    block_number,
    file_name,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS complete_{{ trimmed_model }}_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__' ~ trimmed_model) }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__' ~ trimmed_model ~ '_fr') }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY block_number ORDER BY _inserted_timestamp DESC)) = 1