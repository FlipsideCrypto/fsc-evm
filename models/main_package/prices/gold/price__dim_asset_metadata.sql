{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'dim_asset_metadata_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(asset_id, token_address, symbol, name),SUBSTRING(asset_id, token_address, symbol, name)",
    tags = ['gold','prices','phase_3']
) }}

{# Main query starts here #}
SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    {{ dbt_utils.generate_surrogate_key(['complete_provider_asset_metadata_id']) }} AS dim_asset_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}
{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}

qualify row_number() over (partition by dim_asset_metadata_id order by modified_timestamp desc) = 1