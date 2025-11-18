{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('silver_reads__lido_reads') }}
{{ config (
    materialized = "incremental",
    unique_key = "contract_reads_daily_records_id",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','contract_reads','records','phase_4']
) }}

{% set models = [] %}
{% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    {% set _ = models.append(ref('silver_reads__lido_reads')) %}
{% endif %}
{% set _ = models.append(ref('silver_reads__uniswap_v2_reads')) %}
{% set _ = models.append(ref('silver_reads__uniswap_v3_reads')) %}
{% set _ = models.append(ref('silver_reads__aave_v1_reads')) %}
{% set _ = models.append(ref('silver_reads__aave_v2_reads')) %}
{% set _ = models.append(ref('silver_reads__aave_v3_reads')) %}

WITH all_records AS (
    {% for model in models %}
        SELECT
            contract_address,
            address,
            function_name,
            function_sig,
            input,
            metadata,
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
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS contract_reads_daily_records_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_records qualify (ROW_NUMBER() over (PARTITION BY contract_reads_daily_records_id
ORDER BY
    modified_timestamp DESC)) = 1
