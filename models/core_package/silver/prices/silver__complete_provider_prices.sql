{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ ',\n' %}
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
    tags = ['core']
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
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['p.complete_provider_prices_id']) }} AS complete_provider_prices_id,
    '{{ invocation_id }}' AS _invocation_id
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