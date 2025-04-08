{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','prices','provider','phase_3']
) }}

{# Main query starts here #}
SELECT
    asset_id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_provider_prices'
    ) }}
    -- prices for all ids, no filter necessary