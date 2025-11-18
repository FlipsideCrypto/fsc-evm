{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "incremental",
    unique_key = "contract_reads_daily_records_id",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','contract_reads','records','phase_4']
) }}

WITH lido AS (

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
    FROM
        {{ ref('silver_reads__lido_reads') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
uniswap_v2 AS (
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
    FROM {{ ref('silver_reads__uniswap_v2_reads') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
uniswap_v3 AS (
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
    FROM {{ ref('silver_reads__uniswap_v3_reads') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
aave_v1 AS (
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
    FROM {{ ref('silver_reads__aave_v1_reads') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
aave_v2 AS (
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
    FROM {{ ref('silver_reads__aave_v2_reads') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),
aave_v3 AS (
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
    FROM {{ ref('silver_reads__aave_v3_reads') }}
    {% if is_incremental() %}
    WHERE modified_timestamp > (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
),

all_records AS (
    SELECT
        *
    FROM
        lido
    UNION ALL
    SELECT
        *
    FROM
        uniswap_v2
    UNION ALL
    SELECT
        *
    FROM
        uniswap_v3
    UNION ALL
    SELECT
        *
    FROM
        aave_v1
    UNION ALL
    SELECT
        *
    FROM
        aave_v2
    UNION ALL
    SELECT
        *
    FROM
        aave_v3
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
