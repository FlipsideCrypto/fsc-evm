{# Set variables #}
{%- set source_name = 'ETH_BALANCES' -%}
{%- set model_type = 'COMPLETE' -%}

{%- set full_refresh_type = var((source_name ~ '_complete_full_refresh').upper(), False) -%}

{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_eth_balances_id)" %}

{# Log configuration details #}
{{ log_complete_details(
    post_hook = post_hook,
    full_refresh_type = full_refresh_type
) }}

{# Set up dbt configuration #}
-- depends on: {{ ref('bronze__' ~ source_name.lower()) }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_eth_balances_id",
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
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address']
    ) }} AS complete_eth_balances_id,
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

qualify(ROW_NUMBER() over (PARTITION BY complete_eth_balances_id
ORDER BY
    _inserted_timestamp DESC)) = 1
