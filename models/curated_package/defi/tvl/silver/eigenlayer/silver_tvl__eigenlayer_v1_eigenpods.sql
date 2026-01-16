{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'eigenlayer_v1_eigenpods_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

SELECT
    DISTINCT LOWER(
        decoded_log :eigenPod :: STRING
    ) AS eigenpod_address,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    {{ dbt_utils.generate_surrogate_key(['eigenpod_address']) }} AS eigenlayer_v1_eigenpods_id
FROM
    {{ ref('core__ez_decoded_event_logs') }}
WHERE
    contract_address = LOWER('0x91e677b07f7af907ec9a428aafa9fc14a0d3a338')
    AND event_name = 'PodDeployed'
    AND block_number >= 17445564 -- Contract deployment block

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY eigenlayer_v1_eigenpods_id
    ORDER BY
        modified_timestamp DESC
) = 1
