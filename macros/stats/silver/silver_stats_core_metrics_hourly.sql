{% macro silver_stats_core_metrics_hourly() %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("incremental_strategy: " ~ config.get('incremental_strategy'), info=True) }}
    {{ log("unique_key: " ~ config.get('unique_key'), info=True) }}
    {{ log("cluster_by: " ~ config.get('cluster_by'), info=True) }}
    {{ log("tags: " ~ config.get('tags'), info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['curated']
) }}

{# run incremental timestamp value first then use it as a static value #}
{% if execute %}

{% if is_incremental() %}
{% set query %}
SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('silver__transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

{# Main query starts here #}
SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    MIN(block_number) AS block_number_min,
    MAX(block_number) AS block_number_max,
    COUNT(
        DISTINCT block_number
    ) AS block_count,
    COUNT(
        DISTINCT tx_hash
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_success THEN tx_hash
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_success THEN tx_hash
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT from_address
    ) AS unique_from_count,
    COUNT(
        DISTINCT to_address
    ) AS unique_to_count,
    SUM(tx_fee_precise) AS total_fees,
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }}
WHERE
    block_timestamp_hour < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
{% endmacro %}
