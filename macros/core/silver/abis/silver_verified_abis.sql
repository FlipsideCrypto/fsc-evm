{% macro silver_verified_abis(
        block_explorer,
        streamline = false
    ) %}
    {% set block_explorer = var('BLOCK_EXPLORER') %}
    WITH {% if not streamline %}
        base AS (
            SELECT
                contract_address,
                PARSE_JSON(
                    abi_data :data :result
                ) AS DATA,
                _inserted_timestamp
            FROM
                {{ ref('bronze_api__contract_abis') }}
            WHERE
                abi_data :data :message :: STRING = 'OK'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
block_explorer_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        block_explorer AS abi_source
    FROM
        base
),
{% else %}
    block_explorer_abis AS (
        SELECT
            block_number,
            COALESCE(
                VALUE :"CONTRACT_ADDRESS" :: STRING,
                VALUE :"contract_address" :: STRING
            ) AS contract_address,
            TRY_PARSE_JSON(DATA) AS DATA,
            VALUE,
            block_explorer AS abi_source,
            _inserted_timestamp
        FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND TRY_PARSE_JSON(DATA) :: STRING <> '[]'
    AND TRY_PARSE_JSON(DATA) IS NOT NULL
{% else %}
    {{ ref('bronze__streamline_fr_contract_abis') }}
WHERE
    TRY_PARSE_JSON(DATA) :: STRING <> '[]'
    AND TRY_PARSE_JSON(DATA) IS NOT NULL
{% endif %}

qualify (ROW_NUMBER() over (PARTITION BY contract_address, block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
{% endif %}

user_abis AS (
    SELECT
        contract_address,
        abi,
        discord_username,
        _inserted_timestamp,
        'user' AS abi_source,
        abi_hash
    FROM
        {{ ref('silver__user_verified_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
        WHERE
            abi_source = 'user'
    )
    AND contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        NULL AS discord_username,
        SHA2(DATA) AS abi_hash
    FROM
        block_explorer_abis
    UNION
    SELECT
        contract_address,
        PARSE_JSON(abi) AS DATA,
        _inserted_timestamp,
        'user' AS abi_source,
        discord_username,
        abi_hash
    FROM
        user_abis
)
SELECT
    contract_address,
    DATA,
    _inserted_timestamp,
    abi_source,
    discord_username,
    abi_hash
FROM
    all_abis

    {% if streamline %}
WHERE
    DATA :: STRING <> 'Unknown Exception'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
{% endmacro %}
