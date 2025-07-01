{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% if vars.BALANCES_SL_NEW_BUILD_ENABLED %}

    SELECT
        0 AS block_number
{% else %}

WITH source AS (
    SELECT
        block_number,
        tx_position,
        address,
        pre_balance_precise,
        LAG(
            post_balance_precise,
            1
        ) over (
            PARTITION BY address
            ORDER BY
                block_number,
                tx_position ASC
        ) AS prev_post_balance_precise,
        pre_balance_precise - prev_post_balance_precise AS diff
    FROM
        {{ ref("test_gold__fact_balances_native_recent") }}
        WHERE
            block_timestamp > DATEADD('day', -5, SYSDATE())
        {% if vars.BALANCES_EXCLUSION_LIST_ENABLED %}
        AND address NOT IN (
            SELECT
                DISTINCT address
            FROM
                silver.validator_addresses
        )
    {% endif %}
)
SELECT
    DISTINCT block_number
FROM
    source
WHERE
    diff <> 0
    AND diff IS NOT NULL 

{% endif %}