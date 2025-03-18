{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = get_path_tags(model)
) }}

{# Main query starts here #}
SELECT
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_native_asset_metadata'
    ) }}
WHERE
    blockchain IN ({% if vars.MAIN_PRICES_NATIVE_BLOCKCHAINS is string %}
        '{{ vars.MAIN_PRICES_NATIVE_BLOCKCHAINS }}'
    {% else %}
        {{ vars.MAIN_PRICES_NATIVE_BLOCKCHAINS | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND symbol IN ({% if vars.MAIN_PRICES_NATIVE_SYMBOLS is string %}
        '{{ vars.MAIN_PRICES_NATIVE_SYMBOLS }}'
    {% else %}
        {{ vars.MAIN_PRICES_NATIVE_SYMBOLS | replace('[', '') | replace(']', '') }}
    {% endif %})