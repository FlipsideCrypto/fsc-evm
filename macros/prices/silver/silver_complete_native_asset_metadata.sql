{% macro silver_complete_native_asset_metadata() %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("incremental_strategy: " ~ config.get('incremental_strategy'), info=True) }}
    {{ log("unique_key: " ~ config.get('unique_key'), info=True) }}
    {{ log("post_hook: " ~ config.get('post_hook'), info=True) }}
    {{ log("tags: " ~ config.get('tags'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_native_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, symbol, name),SUBSTRING(asset_id, symbol, name)",
    tags = ['non_realtime']
) }}

{# Main query starts here #}
SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id,
    _invocation_id
FROM
    {{ ref(
        'bronze__complete_native_asset_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
{% endmacro %}
