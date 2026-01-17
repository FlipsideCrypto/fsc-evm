{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'sky_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH a AS (
    SELECT
        block_timestamp::DATE AS date,
        SUM(CASE WHEN
            to_address = '0x0000000000000000000000000000000000000000'
        THEN COALESCE(amount, 0) END) AS sky_burned,
        SUM(CASE WHEN
            from_address = '0x0000000000000000000000000000000000000000'
        THEN COALESCE(amount, 0) END) AS sky_minted
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE contract_address = LOWER('0x56072C95FAA701256059aa122697B133aDEd9279')
    AND (
        from_address = '0x0000000000000000000000000000000000000000'
        OR to_address = '0x0000000000000000000000000000000000000000'
    )
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY 1
),

date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM a) AND TO_DATE(SYSDATE())
    {% if is_incremental() %}
    AND date >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

sparse AS (
    SELECT
        ds.date,
        COALESCE(sky_minted, 0) AS sky_minted,
        COALESCE(sky_burned, 0) AS sky_burned
    FROM date_spine ds
    LEFT JOIN a USING(date)
)

SELECT
    date,
    sky_minted,
    sky_burned,
    SUM(sky_minted - sky_burned) OVER (ORDER BY date ASC) AS total_supply_sky,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    sparse
