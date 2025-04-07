{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','prices','token']
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
    blockchain IN ({% if vars.MAIN_PRICES_TOKEN_BLOCKCHAINS is string %}
        '{{ vars.MAIN_PRICES_TOKEN_BLOCKCHAINS }}'
    {% else %}
        {{ vars.MAIN_PRICES_TOKEN_BLOCKCHAINS | replace('[', '') | replace(']', '') }}
    {% endif %})
    {% if vars.MAIN_PRICES_TOKEN_ADDRESSES is not none %}
        AND token_address IN ({% if vars.MAIN_PRICES_TOKEN_ADDRESSES is string %}
            '{{ vars.MAIN_PRICES_TOKEN_ADDRESSES }}'
        {% else %}
            {{ vars.MAIN_PRICES_TOKEN_ADDRESSES | replace('[', '') | replace(']', '') }}
        {% endif %})
    {% endif %}