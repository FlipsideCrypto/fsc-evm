{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','prices','token','phase_3']
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
    is_verified,
    is_verified_modified_timestamp,
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