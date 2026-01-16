{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'sky_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH collateral_joins AS (
    SELECT
        join_address,
        LOWER(CONCAT('0x', SUBSTR(result_hex, -40))) AS token_address
    FROM
        {{ ref('silver_reads__sky_v1_collateral_joins') }}
    WHERE
        result_hex IS NOT NULL
        AND LENGTH(result_hex) >= 42
        AND result_hex NOT IN ('0x', '0x0000000000000000000000000000000000000000000000000000000000000000')
    {% if is_incremental() %}
        AND modified_timestamp > (SELECT MAX(modified_timestamp) FROM {{ this }})
    {% endif %}
    UNION ALL
    SELECT
        '0x37305b1cd40574e4c5ce33f8e8306be057fd7341' AS join_address, -- Sky: PSM
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS token_address  -- USDC
)

SELECT
    token_address AS contract_address,
    join_address AS address,
    'balanceOf' AS function_name,
    '0x70a08231' AS function_sig,
    CONCAT(
        function_sig,
        LPAD(SUBSTR(join_address, 3), 64, '0')
    ) AS input,
    NULL::VARIANT AS metadata,
    'sky' AS protocol,
    'v1' AS version,
    CONCAT(protocol, '-', version) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS sky_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    collateral_joins
WHERE
    token_address IS NOT NULL
    AND token_address != '0x0000000000000000000000000000000000000000'
