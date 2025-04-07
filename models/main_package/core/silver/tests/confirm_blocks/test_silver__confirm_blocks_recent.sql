{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_silver','core','confirm_blocks','recent_test']
) }}

{%- set default_hours = -24 * 5 -%}
{%- set hour_lookback = default_hours -%}

{% if execute %}
    {% set lookback_query %}
        SELECT
            IFF(
                DATEDIFF('hour', MIN(modified_timestamp), SYSDATE()) <= (24 * 7),
                -12,
                {{ default_hours }}
            ) as hour_lookback
        FROM
            {{ ref('silver__confirm_blocks') }}
    {% endset %}
    
    {% set results = run_query(lookback_query) %}
    {% set hour_lookback = results.columns[0].values()[0] %}
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
