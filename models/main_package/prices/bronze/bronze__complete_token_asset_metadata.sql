{# Set variables #}
{%- set token_addresses = get_var('PRICES_TOKEN_ADDRESSES', none) -%}
{%- set blockchains = get_var('PRICES_TOKEN_BLOCKCHAINS', get_var('GLOBAL_PROD_DB_NAME', '').lower() ) -%}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_prices']
) }}

{# Main query starts here #}
SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_asset_metadata'
    ) }}
WHERE
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    {% if token_addresses is not none %}
        AND token_address IN ({% if token_addresses is string %}
            '{{ token_addresses }}'
        {% else %}
            {{ token_addresses | replace('[', '') | replace(']', '') }}
        {% endif %})
    {% endif %}