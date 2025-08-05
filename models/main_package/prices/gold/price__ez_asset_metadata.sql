{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name)",
    tags = ['gold','prices','phase_3']
) }}

{# Main query starts here #}
SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    FALSE AS is_native,
    is_deprecated,
    is_verified,
    is_verified_modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['complete_token_asset_metadata_id']) }} AS ez_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_token_asset_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    NULL AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    TRUE AS is_native,
    is_deprecated,
    TRUE AS is_verified,
    null as is_verified_modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['complete_native_asset_metadata_id']) }} AS ez_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_native_asset_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}

qualify row_number() over (partition by ez_asset_metadata_id order by modified_timestamp desc) = 1