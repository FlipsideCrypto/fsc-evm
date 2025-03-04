{% if var('GLOBAL_PROD_DB_NAME') != 'ethereum' %}
    -- depends on: {{ ref('bronze__contract_abis') }}
    -- depends on: {{ ref('bronze__contract_abis_fr') }}
{% else %}
    -- depends on: {{ ref('bronze__streamline_contract_abis') }}
    -- depends on: {{ ref('bronze__streamline_fr_contract_abis') }}
{% endif %}

{# Prod DB Variables Start #}
{# Columns included by default, with specific exclusions #}
{% set excludes_etherscan = ['INK', 'SWELL', 'RONIN', 'BOB'] %}

{# Columns excluded by default, with explicit inclusion #}
{% set includes_result_output_abi = ['RONIN'] %}

{# Set Variables using inclusions and exclusions #}
{% set uses_etherscan = var('GLOBAL_PROD_DB_NAME').upper() not in excludes_etherscan %}
{% set uses_result_output_abi = var('GLOBAL_PROD_DB_NAME').upper() in includes_result_output_abi %}
{# Prod DB Variables End #}

{% set abi_block_explorer_name = var(
    'BLOCK_EXPLORER_NAME',
    ''
) %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['silver_abis','phase_2']
) }}

WITH base AS (

    SELECT
        contract_address,
        {% if uses_etherscan %}
            PARSE_JSON(
                abi_data :data :result
            ) AS DATA,
            {% elif uses_result_output_abi %}
            PARSE_JSON(
                abi_data :data :result :output :abi
            ) AS DATA,
        {% else %}
            PARSE_JSON(
                abi_data :data :abi
            ) AS DATA,
        {% endif %}

        _inserted_timestamp
    FROM

{% if is_incremental() %}
{% if var('GLOBAL_PROD_DB_NAME') != 'ethereum' %}
    -- edge case for ethereum
    {{ ref('bronze__contract_abis') }}
{% else %}
    {{ ref('bronze__streamline_contract_abis') }}
{% endif %}
{% else %}
    {% if var('GLOBAL_PROD_DB_NAME') != 'ethereum' %}
        {{ ref('bronze__contract_abis_fr') }}
    {% else %}
        {{ ref('bronze__streamline_fr_contract_abis') }}
    {% endif %}
{% endif %}
WHERE
    {% if uses_etherscan %}
        abi_data :data :message :: STRING = 'OK' {% elif uses_result_output_abi %}
        abi_data :data :result IS NOT NULL
    {% else %}
        abi_data :data :abi IS NOT NULL
    {% endif %}

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP)
    FROM
        {{ this }})
    {% endif %}
),
block_explorer_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        LOWER('{{ abi_block_explorer_name }}') AS abi_source
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
            COALESCE(MAX(_inserted_timestamp), '1970-01-01')
        FROM
            {{ this }}
        WHERE
            abi_source = 'user')
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
    all_abis qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
