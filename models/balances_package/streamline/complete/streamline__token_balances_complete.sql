{# Set variables #}
{%- set source_name = 'TOKEN_BALANCES' -%}
{%- set model_type = 'COMPLETE' -%}

{%- set full_refresh_type = var((source_name ~ '_complete_full_refresh').upper(), false) -%}

{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_token_balances_id)" %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
-- depends on: {{ ref('bronze__' ~ source_name.lower()) }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_token_balances_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = post_hook,
    incremental_predicates = ["dynamic_range", "block_number"],
    full_refresh = full_refresh_type,
    tags = ['streamline_balances_complete']
) }}

{# Main query starts here #}
SELECT
    block_number,
    COALESCE(
        VALUE :"ADDRESS" :: STRING,
        VALUE :"address" :: STRING
    ) AS address,
    COALESCE(
        VALUE :"CONTRACT_ADDRESS" :: STRING,
        VALUE :"contract_address" :: STRING
    ) AS contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address', 'contract_address']
    ) }} AS complete_token_balances_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__' ~ source_name.lower()) }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__' ~ source_name.lower() ~ '_fr') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_token_balances_id
ORDER BY
    _inserted_timestamp DESC)) = 1
