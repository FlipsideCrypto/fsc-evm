{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends_on: {{ ref('bronze__balances_erc20') }}

{{ config (
    materialized = "incremental",
    unique_key = "balances_erc20_daily_complete_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(address,contract_address)",
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','balances','complete','erc20','phase_4']
) }}
{# Main query starts here #}

SELECT
    VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
    (
        VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
    ) :: DATE AS block_date,
    VALUE :"ADDRESS" :: STRING AS address,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address', 'contract_address']) }} AS balances_erc20_daily_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__balances_erc20') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__balances_erc20_fr') }}
        {% endif %}

        qualify (ROW_NUMBER() over (PARTITION BY balances_erc20_daily_complete_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
