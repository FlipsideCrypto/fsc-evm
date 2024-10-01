{% macro silver_complete_provider_prices() %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("incremental_strategy: " ~ config.get('incremental_strategy'), info=True) }}
    {{ log("unique_key: " ~ config.get('unique_key'), info=True) }}
    {{ log("cluster_by: " ~ config.get('cluster_by'), info=True) }}
    {{ log("post_hook: " ~ config.get('post_hook'), info=True) }}
    {{ log("tags: " ~ config.get('tags'), info=True) }}
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
