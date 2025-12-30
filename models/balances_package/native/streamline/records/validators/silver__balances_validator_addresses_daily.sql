{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{# Set up dbt configuration #}
{{ config (
    materialized = "incremental",
    unique_key = "balances_validator_addresses_daily_id",
    cluster_by = "block_date",
    tags = ['silver','balances','records','native','phase_4']
) }}

WITH miner_addresses AS (
        SELECT
            DISTINCT block_timestamp :: DATE AS block_date,
            miner AS address
        FROM
            {{ ref('core__fact_blocks') }}
        WHERE miner <> '0x0000000000000000000000000000000000000000'
        {% if is_incremental() %}
        AND 
            modified_timestamp > (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
    )
SELECT
    block_date,
    address,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address']) }} AS balances_validator_addresses_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    miner_addresses
QUALIFY(ROW_NUMBER() over (PARTITION BY balances_validator_addresses_daily_id
ORDER BY
    modified_timestamp DESC)) = 1