-- depends on: {% if var('GLOBAL_PROD_DB_NAME') != 'ethereum' %}{{ ref('bronze__contract_abis') }}{% else %}{{ ref('bronze__streamline_contract_abis') }}{% endif %}
{% if var('GLOBAL_PROD_DB_NAME') != 'ethereum' %}
    {{ config (
        materialized = 'incremental',
        unique_key = 'complete_contract_abis_id',
        cluster_by = 'partition_key',
        post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_contract_abis_id, contract_address)",
        incremental_predicates = ['dynamic_range', 'partition_key'],
        tags = ['streamline_abis_complete']
    ) }}

    SELECT
        partition_key,
        contract_address,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }} AS complete_contract_abis_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE (MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__contract_abis_fr') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY complete_contract_abis_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
