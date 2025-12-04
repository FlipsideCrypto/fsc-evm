{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'curve_reads_id',
    tags = ['silver','contract_reads','curated_daily']
) }}

WITH liquidity_pools AS (

    SELECT
        DISTINCT pool_address AS address,
        token_address AS contract_address,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_dex__curve_pools') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    contract_address,
    address,
    'balanceOf' AS function_name,
    '0x70a08231' AS function_sig,
    CONCAT(
        '0x70a08231',
        LPAD(SUBSTR(address, 3), 64, '0')
    ) AS input,
    NULL :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS curve_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    liquidity_pools
