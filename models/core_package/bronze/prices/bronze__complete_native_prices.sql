{%- if var('GLOBAL_USES_V2_FSC_EVM', False) -%}

{# Set variables #}
{%- set symbols = var('PRICES_SYMBOLS') -%}
{%- set blockchains = var('PRICES_BLOCKCHAINS', var('GLOBAL_PROD_DB_NAME').lower() ) -%}

{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log("PRICES_SYMBOLS: " ~ symbols, info=True) }}
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
    HOUR,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_prices'
    ) }}
WHERE
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND symbol IN ({% if symbols is string %}
        '{{ symbols }}'
    {% else %}
        {{ symbols | replace('[', '') | replace(']', '') }}
    {% endif %})
{%- endif -%}