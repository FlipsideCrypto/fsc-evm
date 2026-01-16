{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'convex', 'treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Convex Treasury Balance

    Tracks token balances in Convex treasury addresses:
    - 0x1389388d01708118b497f59521f6943Be2541bb7
    - 0xe98984aD858075813AdA4261aF47e68A64E28fCC

    Active since: May 12, 2021
#}

WITH base AS (
    {{ get_treasury_balance(
        'ethereum',
        [
            '0x1389388d01708118b497f59521f6943Be2541bb7',
            '0xe98984aD858075813AdA4261aF47e68A64E28fCC'
        ],
        '2021-05-12',
        is_incremental(),
        vars.CURATED_LOOKBACK_HOURS,
        vars.CURATED_LOOKBACK_DAYS
    ) }}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM base
