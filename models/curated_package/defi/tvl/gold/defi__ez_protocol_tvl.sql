{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_protocol_tvl_id',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','defi','tvl','curated_daily']
) }}

WITH complete_tvl AS (

    SELECT
        block_number,
        block_date,
        contract_address,
        address,
        token_address,
        decimals,
        symbol,
        is_verified,
        max_usd_exponent,
        amount_hex,
        amount_raw,
        amount_precise,
        amount,
        CASE 
            WHEN t.amount_usd < POWER(10, t.max_usd_exponent) THEN t.amount_usd
            ELSE NULL
        END AS amount_usd,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_tvl__complete_tvl') }} t

{% if is_incremental() %}
WHERE
    CONCAT(
        block_date,
        '-',
        platform
    ) IN (
        SELECT
            DISTINCT CONCAT(
                block_date,
                '-',
                platform
            )
        FROM
            {{ ref('silver_tvl__complete_tvl') }}
        WHERE
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
    )
{% endif %}
)
SELECT
    block_date,
    SUM(COALESCE(amount_usd, 0)) AS tvl_usd,
    protocol,
    version,
    platform,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','platform']
    ) }} AS ez_protocol_tvl_id
FROM
    complete_tvl
GROUP BY
    block_date,
    protocol,
    version,
    platform
