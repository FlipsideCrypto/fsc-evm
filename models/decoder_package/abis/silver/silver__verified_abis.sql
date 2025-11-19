{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__contract_abis') }}

{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','abis','phase_2']
) }}

WITH base AS (

    SELECT
        contract_address,
        {% if vars.DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED %}
            PARSE_JSON(
                VALUE :data :result
            ) AS DATA,
        {% elif vars.DECODER_SILVER_CONTRACT_ABIS_RESULT_ENABLED %}
            PARSE_JSON(
                VALUE :data :result :output :abi
            ) AS DATA,
        {% else %}
            PARSE_JSON(
                VALUE :data :abi
            ) AS DATA,
        {% endif %}

        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01')
        FROM
            {{ this }}
            )
        AND {% if vars.DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED %}
                VALUE :data :message :: STRING = 'OK' 
            {% elif vars.DECODER_SILVER_CONTRACT_ABIS_RESULT_ENABLED %}
                VALUE :data :result IS NOT NULL
            {% else %}
                VALUE :data :abi IS NOT NULL
            {% endif %}
        {% else %}
            {{ ref('bronze__contract_abis_fr') }}
        WHERE
            {% if vars.DECODER_SILVER_CONTRACT_ABIS_ETHERSCAN_ENABLED %}
                VALUE :data :message :: STRING = 'OK' 
            {% elif vars.DECODER_SILVER_CONTRACT_ABIS_RESULT_ENABLED %}
                VALUE :data :result IS NOT NULL
            {% else %}
                VALUE :data :abi IS NOT NULL
            {% endif %}
        {% endif %}
    ),
    block_explorer_abis AS (
        SELECT
            contract_address,
            DATA,
            _inserted_timestamp,
            LOWER('{{ vars.DECODER_SILVER_CONTRACT_ABIS_EXPLORER_NAME }}') AS abi_source
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
