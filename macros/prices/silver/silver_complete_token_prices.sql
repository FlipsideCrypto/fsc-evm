{% macro silver_complete_token_prices() %}

{# Log configuration details if in dev, during execution #}
{%- if execute and not target.name.startswith('prod') -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}    
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
    {% set config_log = config_log ~ '    post_hook = "' ~ config.get('post_hook') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_token_prices_id',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)",
    tags = ['non_realtime']
) }}

{# Main query starts here #}
SELECT
    HOUR,
    LOWER(
        p.token_address
    ) AS token_address,
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
    {{ ref(
        'bronze__complete_token_prices'
    ) }}
    p

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
{% endmacro %}
