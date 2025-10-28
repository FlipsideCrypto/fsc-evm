{% test curated_recency_defi(
    model,
    threshold_days=30,
    percent_delta_threshold=10,
    type=''
) %}

{# Get variables #}
{% set vars = return_vars() %}

WITH source AS (
    SELECT
        platform,
        MAX(block_timestamp) AS latest_timestamp,
        SYSDATE() AS sys_ts,
        DATEADD('day', -{{ threshold_days }}, SYSDATE()) AS threshold_ts,
        COUNT(
            CASE
                WHEN block_timestamp >= threshold_ts THEN 1 
            END) AS current_period_evt,
        CEIL(
            (
                COUNT(
                    CASE
                        WHEN block_timestamp >= DATEADD('day', -180, SYSDATE())
                        AND block_timestamp < threshold_ts THEN 1
                    END
                    ) / 150.0
                ) * 30
            ) AS rolling_avg_evt,
        CASE
            WHEN rolling_avg_evt = 0 THEN 0
            ELSE ROUND((current_period_evt / rolling_avg_evt) * 100, 2)
        END AS percent_delta
    FROM
        {{ model }}
    GROUP BY
        1
        )
        SELECT
            platform,
            latest_timestamp,
            sys_ts,
            threshold_ts,
            current_period_evt,
            rolling_avg_evt,
            percent_delta
        FROM
            source
        WHERE
            (latest_timestamp < threshold_ts 
            OR (percent_delta < {{ percent_delta_threshold }} AND percent_delta <> 0))
            {% if type == 'dex_swaps' %}
            AND platform NOT IN ('{{ vars.CURATED_DEFI_DEX_SWAPS_RECENCY_EXCLUSION_LIST | join("', '") }}')
            {% elif type == 'dex_lp_actions' %}
            AND platform NOT IN ('{{ vars.CURATED_DEFI_DEX_LP_ACTIONS_RECENCY_EXCLUSION_LIST | join("', '") }}')
            {% elif type == 'bridge' %}
            AND platform NOT IN ('{{ vars.CURATED_DEFI_BRIDGE_RECENCY_EXCLUSION_LIST | join("', '") }}')
            {% elif type == 'lending' %}
            AND platform NOT IN ('{{ vars.CURATED_DEFI_LENDING_RECENCY_EXCLUSION_LIST | join("', '") }}')
            {% endif %}
-- failure to meet threshold requires manual review to determine if
-- the protocol has newly deployed contracts, stale contracts, etc.
-- if this is the case, the test should not apply to the relevant model(s)
-- percent_delta shows current period performance vs rolling average
{% endtest %}
