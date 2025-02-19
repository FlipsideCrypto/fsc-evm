-- depends on: {{ ref('bronze__contract_abis') }}
{{ config (
    materialized = 'incremental',
    unique_key = 'complete_contract_abis_id',
    cluster_by = 'partition_key',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_contract_abis_id, contract_address)",
    incremental_predicates = ['dynamic_range', 'partition_key'],
    tags = ['streamline_abis_complete']
) }}

{% if is_incremental() %}

SELECT
    partition_key,
    COALESCE(
        VALUE :"CONTRACT_ADDRESS" :: STRING,
        VALUE :"contract_address" :: STRING
    ) AS contract_address,
    VALUE AS abi_data,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS complete_contract_abis_id,
    _inserted_timestamp
FROM
    {{ ref('bronze__contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
SELECT
    partition_key,
    COALESCE(
        VALUE :"CONTRACT_ADDRESS" :: STRING,
        VALUE :"contract_address" :: STRING
    ) AS contract_address,
    VALUE AS abi_data,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS complete_contract_abis_id,
    _inserted_timestamp
FROM
    {{ ref('bronze__contract_abis_fr') }}

    {% if var( 'BLOCK_EXPLORER_ABI_USES_BRONZE_API_TABLE',false ) %}
    UNION ALL
    SELECT
        1 AS partition_key,
        contract_address,
        abi_data,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }} AS complete_contract_abis_id,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__contract_abis') }}
    {% else %}
    UNION ALL
    SELECT
        1 AS partition_key,
        '0x0000000000000000000000000000000000000000' AS contract_address,
        NULL as abi_data,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }} AS complete_contract_abis_id,
        '2000-01-01'::timestamp as _inserted_timestamp
    {% endif %}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_contract_abis_id
ORDER BY
    _inserted_timestamp DESC)) = 1
