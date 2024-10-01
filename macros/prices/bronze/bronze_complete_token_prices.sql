{% macro bronze_complete_token_prices() %}

{# Set macro parameters #}
{%- set token_addresses = var('PRICES_TOKEN_ADDRESSES', token_addresses) -%}
{%- set blockchains = var('PRICES_BLOCKCHAINS', target.database | lower | replace('_dev', '') ) -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view'
) }}

{# Main query starts here #}
SELECT
    HOUR,
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_prices'
    ) }}
WHERE
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    {% if token_addresses %}
        AND token_address IN ({% if token_addresses is string %}
            '{{ token_addresses }}'
        {% else %}
            {{ token_addresses | replace('[', '') | replace(']', '') }}
        {% endif %})
    {% endif %}
{% endmacro %}
