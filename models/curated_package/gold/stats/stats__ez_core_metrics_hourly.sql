{# Set variables #}
{%- set token_address = var('STATS_TOKEN_ADDRESS', '') -%}

{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}

    {{ log("STATS_TOKEN_ADDRESS: " ~ token_address, info=True) }}
    {{ log("", info=True) }}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    meta = ' ~ config.get('meta') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } },
    tags = ['curated']
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