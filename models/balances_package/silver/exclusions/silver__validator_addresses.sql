{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['address'],
    incremental_strategy = 'delete+insert',
    tags = ['silver','balances','phase_4']
) }}

SELECT
    DISTINCT origin_from_address AS address,
    {{ dbt_utils.generate_surrogate_key(['address']) }} AS validator_addresses_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
FROM
    {{ ref('core__fact_traces') }}
WHERE
    origin_function_signature = '0xf340fa01'
    AND origin_to_address = '{{ vars.BALANCES_VALIDATOR_CONTRACT_ADDRESS }}'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
    FROM
        {{ this }})
        AND address NOT IN (
            SELECT
                DISTINCT address
            FROM
                {{ this }}
        )
    {% endif %}
