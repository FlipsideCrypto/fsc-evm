{% macro silver_complete_provider_prices() %}

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
    unique_key = 'complete_provider_prices_id',
    cluster_by = ['recorded_hour::DATE','provider'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id),SUBSTRING(asset_id)",
    tags = ['non_realtime']
) }}

{# Main query starts here #}
SELECT
    p.asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    p.provider,
    p.source,
    p._inserted_timestamp,
    p.inserted_timestamp,
    p.modified_timestamp,
    p.complete_provider_prices_id,
    p._invocation_id
FROM
    {{ ref(
        'bronze__complete_provider_prices'
    ) }}
    p
    INNER JOIN {{ ref('bronze__complete_provider_asset_metadata') }}
    m
    ON p.asset_id = m.asset_id

{% if is_incremental() %}
WHERE
    p.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY p.asset_id, recorded_hour, p.provider
ORDER BY
    p.modified_timestamp DESC)) = 1
{% endmacro %}
