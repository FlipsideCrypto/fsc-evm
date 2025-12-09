{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'stablecoins_reads_id',
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','contract_reads']
) }} 

WITH verified_stablecoins AS (

    SELECT
        contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND contract_address IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    contract_address,
    NULL AS address,
    'totalSupply' AS function_name,
    '0x18160ddd' AS function_sig,
    RPAD(
        function_sig,
        64,
        '0'
    ) AS input,
    NULL :: VARIANT AS metadata,
    'stablecoins' AS protocol,
    'v1' AS version,
    CONCAT('stablecoins','-','v1') AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','input','platform']
    ) }} AS stablecoins_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    verified_stablecoins
