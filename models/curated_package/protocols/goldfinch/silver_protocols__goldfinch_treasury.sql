{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date', 'token_address'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'goldfinch', 'treasury', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Goldfinch Treasury

    Tracks token balances in the Goldfinch Treasury address.
    Treasury address: 0xBEb28978B2c755155f20fd3d09Cb37e300A6981f
    Note: TGE on 01/10~01/11 2022 so pricing data before this date is excluded.
#}

WITH treasury AS (
    {{ get_treasury_balance(
        'ethereum',
        '0xBEb28978B2c755155f20fd3d09Cb37e300A6981f',
        '2020-01-01',
        is_incremental(),
        vars.CURATED_LOOKBACK_HOURS,
        vars.CURATED_LOOKBACK_DAYS
    ) }}
)

SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM treasury
WHERE date > DATE('2022-01-11')
{% if is_incremental() %}
AND date >= (
    SELECT MAX(date) - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }} days'
    FROM {{ this }}
)
{% endif %}
