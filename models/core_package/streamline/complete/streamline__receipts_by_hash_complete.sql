{# Set uses_receipts_by_hash based on model configuration #}
{% set uses_receipts_by_hash = var('USES_RECEIPTS_BY_HASH', false) %}

{% if uses_receipts_by_hash %}

{% set source_name = 'RECEIPTS_BY_HASH' %}
{% set model_type = 'COMPLETE' %}

{%- set full_refresh_type = var(('complete_' ~ trimmed_model ~ '_full_refresh').upper(), False) -%}

{# Log configuration details if in dev or during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("USES_RECEIPTS_BY_HASH: " ~ uses_receipts_by_hash, info=True) }}

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

-- depends_on: {{ ref('bronze__' ~ source_name.lower()) }}

{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number, tx_hash)",
    full_refresh = full_refresh_type,
    tags = ['streamline_core_' ~ model_type.lower()]
) }}

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

QUALIFY (ROW_NUMBER() OVER (PARTITION BY block_number, tx_hash ORDER BY _inserted_timestamp DESC)) = 1

{% endif %}
