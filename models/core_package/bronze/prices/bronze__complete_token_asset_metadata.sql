{# Set variables #}
{%- set token_addresses = var('PRICES_TOKEN_ADDRESSES', none) -%}
{%- set blockchains = var('PRICES_BLOCKCHAINS', var('GLOBAL_PROD_DB_NAME', '').lower() ) -%}

{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log("PRICES_TOKEN_ADDRESSES: " ~ token_addresses, info=True) }}
    {{ log("PRICES_BLOCKCHAINS: " ~ blockchains, info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}
    
{%- endif -%}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['core']
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