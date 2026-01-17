{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['date'],
    cluster_by = ['date'],
    tags = ['silver_protocols', 'maker', 'mkr_supply', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH a AS (
    SELECT
        block_timestamp::DATE AS date,
        SUM(CASE WHEN
            event_name = 'Burn'
        THEN COALESCE(decoded_log:wad / 1e18, 0) END) AS mkr_burned,
        SUM(CASE WHEN
            event_name = 'Mint'
        THEN COALESCE(decoded_log:wad / 1e18, 0) END) AS mkr_minted
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = LOWER('0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2')
    AND event_name IN ('Burn', 'Mint')
    {% if is_incremental() %}
    AND block_timestamp >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
    GROUP BY 1
),

date_spine AS (
    SELECT
        date
    FROM {{ ref('utils__date_spine') }}
    WHERE date BETWEEN (SELECT MIN(date) FROM a) AND TO_DATE(SYSDATE())
    {% if is_incremental() %}
    AND date >= DATEADD('day', -{{ vars.CURATED_LOOKBACK_DAYS }}, CURRENT_DATE())
    {% endif %}
),

sparse AS (
    SELECT
        ds.date,
        COALESCE(mkr_minted, 0) AS mkr_minted,
        COALESCE(mkr_burned, 0) AS mkr_burned
    FROM date_spine ds
    LEFT JOIN a USING(date)
)

SELECT
    date,
    mkr_minted,
    mkr_burned,
    SUM(mkr_minted - mkr_burned) OVER (ORDER BY date ASC) AS total_supply_mkr,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    sparse
