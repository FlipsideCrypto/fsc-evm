{# Prod DB Variables Start #}
{# Columns included by default, with specific exclusions #}
{% set excludes_etherscan = ['INK'] %}

{# Columns excluded by default, with explicit inclusion #}

{# Set Variables using inclusions and exclusions #}
{% set uses_etherscan = var('GLOBAL_PROD_DB_NAME').upper() not in excludes_etherscan %}
{# Prod DB Variables End #}

{% set abi_block_explorer_name = var('BLOCK_EXPLORER_NAME','') %}

{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['silver_abis']
) }}

WITH base AS (

    SELECT
        contract_address,
        {% if uses_etherscan %}
        PARSE_JSON(
            abi_data :data :result
        ) AS DATA,
        {% else %}
        PARSE_JSON(
            abi_data :data :abi
        ) AS DATA,
        {% endif %}
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_api',
            'contract_abis'
        ) }}
    WHERE
        {% if uses_etherscan %}
        abi_data :data :message :: STRING = 'OK'
        {% else %}
        abi_data :data :message IS NULL
        {% endif %}

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
        lower('{{ abi_block_explorer_name }}') AS abi_source
    FROM
        base
),
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
            COALESCE(
                MAX(_inserted_timestamp),
                '1970-01-01'
            )
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

qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1