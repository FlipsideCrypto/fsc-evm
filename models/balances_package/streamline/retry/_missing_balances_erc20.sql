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
            contract_address,
            pre_balance_precise,
            LAG(
                post_balance_precise,
                1
            ) over (
                PARTITION BY address,
                contract_address
                ORDER BY
                    block_number,
                    tx_position ASC
            ) AS prev_post_balance_precise,
            pre_balance_precise - prev_post_balance_precise AS diff
        FROM
            {{ ref("test_gold__fact_balances_erc20_recent") }}
        WHERE
            block_timestamp > DATEADD('day', -5, SYSDATE())
    )
SELECT
    DISTINCT block_number
FROM
    source
WHERE
    diff <> 0
    AND diff IS NOT NULL 
{% endif %}