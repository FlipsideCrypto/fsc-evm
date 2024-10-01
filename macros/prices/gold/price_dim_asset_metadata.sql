{% macro price_dim_asset_metadata() %}

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
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}
{% endmacro %}
