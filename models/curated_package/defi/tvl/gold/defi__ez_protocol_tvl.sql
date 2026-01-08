{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','defi','tvl','curated_daily']
) }}

WITH tvl_usd AS (
SELECT
    block_date,
    SUM(
        CASE 
            WHEN COALESCE(amount_usd, 0) < POWER(10, max_usd_exponent) THEN COALESCE(amount_usd, 0)
            ELSE 0
        END
    ) AS tvl_usd,
    protocol,
    version,
    platform,
    MAX(modified_timestamp) AS modified_timestamp,
    MAX(inserted_timestamp) AS inserted_timestamp
FROM
    {{ ref('silver_tvl__complete_tvl') }}
GROUP BY
    block_date,
    protocol,
    version,
    platform
)
SELECT
    block_date,
    tvl_usd,
    protocol,
    version,
    platform,
    modified_timestamp,
    inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','platform']
    ) }} AS ez_protocol_tvl_id
FROM
    tvl_usd