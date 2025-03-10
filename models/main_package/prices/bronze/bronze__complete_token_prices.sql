{# Set variables #}
{%- set token_addresses = var('PRICES_TOKEN_ADDRESSES', none) -%}
{%- set blockchains = var('PRICES_TOKEN_BLOCKCHAINS', var('GLOBAL_PROD_DB_NAME', '').lower() ) -%}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_prices','phase_2']
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
    {% if token_addresses is not none %}
        AND token_address IN ({% if token_addresses is string %}
            '{{ token_addresses }}'
        {% else %}
            {{ token_addresses | replace('[', '') | replace(']', '') }}
        {% endif %})
    {% endif %}
{% if var('MAIN_PRICES_TOKEN_WETH_ENABLED', false) %}
UNION ALL
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
    blockchain = 'ethereum'
    AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
{% endif %}