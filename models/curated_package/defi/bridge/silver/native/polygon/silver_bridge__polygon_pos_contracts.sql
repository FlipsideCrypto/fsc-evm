{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

SELECT
    address,
    NAME,
    modified_timestamp
FROM
    {{ ref('core__dim_contracts') }}
WHERE
    NAME LIKE '%(PoS)%'
    AND '{{ vars.GLOBAL_PROJECT_NAME }}' = 'polygon'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
UNION ALL
SELECT
    '0x0000000000000000000000000000000000001010' AS address,
    'POL' AS NAME,
    TO_TIMESTAMP('1970-01-01') AS modified_timestamp
