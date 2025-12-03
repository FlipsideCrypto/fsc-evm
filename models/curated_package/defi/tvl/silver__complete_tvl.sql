{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'complete_tvl_id',
    tags = ['silver','defi','tvl','complete','curated_daily']
) }}

{% set models = [] %}
{% set _ = models.append(ref('silver__aave_v1_tvl')) %}
{% set _ = models.append(ref('silver__aave_v2_tvl')) %}
{% set _ = models.append(ref('silver__aave_v3_tvl')) %}
{% set _ = models.append(ref('silver__curve_v1_tvl')) %}
{% set _ = models.append(ref('silver__lido_v1_tvl')) %}
{% set _ = models.append(ref('silver__tornado_cash_v1_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v2_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v3_tvl')) %}
{% set _ = models.append(ref('silver__uniswap_v4_tvl')) %}

WITH all_tvl AS (
    {% for model in models %}
        SELECT
            block_number,
            block_date,
            contract_address,
            address,
            amount_hex,
            amount_raw,
            protocol,
            version,
            platform
        FROM {{ model }}
        {% if not loop.last %}
        {% if is_incremental() %}
        WHERE modified_timestamp > (
            SELECT MAX(modified_timestamp)
            FROM {{ this }}
        )
        {% endif %}
        UNION ALL
        {% endif %}
    {% endfor %}
),
final AS (
    SELECT
        a.block_number,
        a.block_date,
        a.contract_address,
        a.address,
        c1.decimals,
        c1.symbol,
        a.amount_hex,
        a.amount_raw,
        utils.udf_decimal_adjust(
            a.amount_raw,
            c1.decimals
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        ROUND(amount * p1.price, 2) AS amount_usd,
        a.protocol,
        a.version,
        a.platform
    FROM
        all_tvl a 
    LEFT JOIN {{ ref('core__dim_contracts') }}
    c1
    ON a.contract_address = c1.address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON a.contract_address = p1.token_address
    AND DATEADD(
      'hour',
      23,
      a.block_date
    ) = p1.hour
)
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    decimals,
    symbol,
    amount_hex,
    amount_raw,
    amount_precise,
    amount,
    amount_usd,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform']
    ) }} AS complete_tvl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    final