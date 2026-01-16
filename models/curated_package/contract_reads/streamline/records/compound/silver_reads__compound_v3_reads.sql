{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'compound_v3_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH base_tokens AS (
    -- Base token (e.g., USDC, WETH) for each Comet market
    SELECT
        underlying_asset_address AS contract_address,
        compound_market_address AS address,
        protocol,
        version,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform
    FROM
        {{ ref('silver_lending__comp_v3_asset_details') }}

{% if is_incremental() %}
WHERE modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

collateral_tokens AS (
    -- All collateral assets for each Comet market
    SELECT
        collateral_asset_address AS contract_address,
        compound_market_address AS address,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_lending__comp_v3_collateral_assets') }}

{% if is_incremental() %}
WHERE modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

all_tokens AS (
    SELECT * FROM base_tokens
    UNION
    SELECT * FROM collateral_tokens
)

SELECT
    contract_address,
    address,
    'balanceOf' AS function_name,
    '0x70a08231' AS function_sig,
    CONCAT(
        function_sig,
        LPAD(SUBSTR(address, 3), 64, '0')
    ) AS input,
    NULL :: variant AS metadata,
    protocol,
    version,
    platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS compound_v3_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_tokens
