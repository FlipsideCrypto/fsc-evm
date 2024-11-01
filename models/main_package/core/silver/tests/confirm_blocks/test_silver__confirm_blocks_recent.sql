{{ config (
    materialized = "view",
    tags = ['recent_test_confirm_blocks']
) }}

{%- set default_hours = -24 * 5 -%}

{%- if execute -%}
    {% set lookback_query %}
        SELECT
            IFF(
                DATEDIFF('hour', MIN(modified_timestamp), SYSDATE()) <= (24 * 6),
                -12,
                {{ default_hours }}
            ) as hour_lookback
        FROM
            {{ ref('silver__confirm_blocks') }}
    {% endset %}
    
    {% set hour_lookback = run_query(lookback_query).rows[0].hour_lookback %}
{%- else -%}
    {% set hour_lookback = default_hours %}
{%- endif -%}

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
