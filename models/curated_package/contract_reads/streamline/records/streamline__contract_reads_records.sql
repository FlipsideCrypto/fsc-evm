{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
-- depends_on: {{ ref('silver_reads__lido_v1_reads') }}
-- depends_on: {{ ref('silver_reads__binance_v1_reads') }}
-- depends_on: {{ ref('silver_reads__polymarket_v1_reads') }}
-- depends_on: {{ ref('silver_reads__eigenlayer_v1_reads') }}
{{ config (
    materialized = "incremental",
    unique_key = "contract_reads_records_id",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    post_hook = '{{ unverify_contract_reads() }}',
    tags = ['streamline','contract_reads','records','heal','phase_4']
) }}

-- only specify chains/exclusions for _reads models with hardcoded or seed driven address data
-- for dynamic models, the underlying upstream data will already be filtered or made relevant for that chain
{% set models = [] %} 
{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    {% set _ = models.append((ref('silver_reads__lido_v1_reads'), 'daily')) %}
    {% set _ = models.append((ref('silver_reads__binance_v1_reads'), 'daily')) %}
    {% set _ = models.append((ref('silver_reads__eigenlayer_v1_reads'), 'daily')) %}
{% endif %}
{% if vars.GLOBAL_PROJECT_NAME == 'polygon' %}
    {% set _ = models.append((ref('silver_reads__polymarket_v1_reads'), 'daily')) %}
{% endif %}
{% set _ = models.append((ref('silver_reads__aerodrome_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__superchain_slipstream_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__stablecoins_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__uniswap_v2_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__uniswap_v3_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__uniswap_v4_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__aave_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__aave_v2_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__aave_v3_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__curve_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__tornado_cash_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__etherfi_v1_reads'), 'daily')) %}
{% set _ = models.append((ref('silver_reads__morpho_blue_v1_reads'), 'daily')) %}

WITH all_records AS (
    {% for model, type in models %}
        SELECT
            contract_address,
            address,
            function_name,
            function_sig,
            input,
            metadata,
            protocol,
            version,
            platform,
            '{{ type }}' AS type
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
)

SELECT
    contract_address,
    address,
    function_name,
    function_sig,
    input,
    metadata,
    protocol,
    version,
    platform,
    type,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS contract_reads_records_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_records qualify (ROW_NUMBER() over (PARTITION BY contract_reads_records_id
ORDER BY
    modified_timestamp DESC)) = 1
