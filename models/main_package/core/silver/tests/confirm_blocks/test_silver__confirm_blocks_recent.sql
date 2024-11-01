{{ config (
    materialized = "view",
    tags = ['recent_test_confirm_blocks']
) }}

{% set lookback_query %}
SELECT
    IFF(
        DATEDIFF('hour', MIN(modified_timestamp), SYSDATE()) <= (
            24 * 6
        ),
        -12,
        -24 * 5
    ) as hour_lookback
FROM
    {{ ref('silver__confirm_blocks') }}
{% endset %}

{% set hour_lookback = run_query(lookback_query) %}

{% if execute %}
    {% set hour_lookback = hour_lookback.rows[0].hour_lookback %}
{% endif %}

SELECT
    *
FROM
    {{ ref('silver__confirm_blocks') }}
WHERE
    modified_timestamp > DATEADD(
        'hour',
        {{ hour_lookback }},
        SYSDATE()
    )
    AND partition_key > (
        SELECT
            ROUND(
                block_number,
                -3
            ) AS block_number
        FROM
            {{ ref('_block_lookback') }}
    )
