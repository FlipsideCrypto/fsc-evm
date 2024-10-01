{% macro price_ez_asset_metadata() %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("persist_docs: " ~ config.get('persist_docs'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

{# Main query starts here #}
SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    FALSE AS is_native,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_token_asset_metadata') }}
UNION ALL
SELECT
    NULL AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    TRUE AS is_native,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}
{% endmacro %}
