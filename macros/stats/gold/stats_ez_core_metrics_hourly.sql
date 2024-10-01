{% macro stats_ez_core_metrics_hourly() %}

{# Set macro parameters #}
{%- set token_address = var('STATS_TOKEN_ADDRESS', token_address) -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("persist_docs: " ~ config.get('persist_docs'), info=True) }}
    {{ log("meta: " ~ config.get('meta'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }
) }}

{# Main query starts here #}
SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    unique_to_count,
    total_fees AS total_fees_native,
    ROUND(
        total_fees * LAST_VALUE(
            p.price ignore nulls
        ) over (
            ORDER BY
                block_timestamp_hour rows unbounded preceding
        ),
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp AS inserted_timestamp,
    s.modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '{{ token_address }}' --Wrapped Native Token Address for target blockchain
{% endmacro %}
