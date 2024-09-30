{% macro stats_ez_core_metrics_hourly(
        token_address
    ) %}
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
