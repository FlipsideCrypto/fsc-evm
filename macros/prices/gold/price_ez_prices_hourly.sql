{% macro price_ez_prices_hourly() %}

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
    HOUR,
    token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    FALSE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_token_prices') }}
UNION ALL
SELECT
    HOUR,
    NULL AS token_address,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}
{% endmacro %}
