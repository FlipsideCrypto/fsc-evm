{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__token_reads') }}
-- depends_on: {{ ref('bronze__token_reads_fr') }}

{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['silver','core','phase_2']
) }}

WITH base_metadata AS (

    SELECT
        contract_address,
        VALUE :"LATEST_BLOCK" :: INT AS block_number,
        VALUE :"FUNCTION_SIG" :: STRING AS function_signature,
        data :result :: STRING AS read_output,
        _inserted_timestamp
    FROM
    {% if is_incremental() %}
        {{ ref('bronze__token_reads') }}
    {% else %}
        {{ ref('bronze__token_reads_fr') }}
    {% endif %}
    WHERE
        read_output IS NOT NULL
        AND read_output <> '0x'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
)
{% endif %}
),
token_names AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        utils.udf_hex_to_string(
            SUBSTR(read_output,(64 * 2 + 3), len(read_output))) AS token_name
            FROM
                base_metadata
            WHERE
                function_signature = '0x06fdde03'
                AND token_name IS NOT NULL
        ),
        token_symbols AS (
            SELECT
                contract_address,
                block_number,
                function_signature,
                read_output,
                utils.udf_hex_to_string(
                    SUBSTR(read_output,(64 * 2 + 3), len(read_output))) AS token_symbol
                    FROM
                        base_metadata
                    WHERE
                        function_signature = '0x95d89b41'
                        AND token_symbol IS NOT NULL
                ),
                token_decimals AS (
                    SELECT
                        contract_address,
                        CASE
                            WHEN read_output IS NOT NULL THEN utils.udf_hex_to_int(
                                read_output :: STRING
                            )
                            ELSE NULL
                        END AS token_decimals,
                        LENGTH(token_decimals) AS dec_length
                    FROM
                        base_metadata
                    WHERE
                        function_signature = '0x313ce567'
                        AND read_output IS NOT NULL
                        AND read_output <> '0x'
                        AND LENGTH(read_output :: STRING) <= 4300
                ),
                contracts AS (
                    SELECT
                        contract_address,
                        MAX(_inserted_timestamp) AS _inserted_timestamp
                    FROM
                        base_metadata
                    GROUP BY
                        1
                ),
                final AS (
            SELECT
                c1.contract_address :: STRING AS contract_address,
                token_name,
                TRY_TO_NUMBER(token_decimals) AS token_decimals,
                token_symbol,
                _inserted_timestamp,
                {{ dbt_utils.generate_surrogate_key(
                    ['c1.contract_address']
                ) }} AS contracts_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp,
                '{{ invocation_id }}' AS _invocation_id
            FROM
                contracts c1
                LEFT JOIN token_names
                ON c1.contract_address = token_names.contract_address
                LEFT JOIN token_symbols
                ON c1.contract_address = token_symbols.contract_address
                LEFT JOIN token_decimals
                ON c1.contract_address = token_decimals.contract_address
                AND dec_length < 3 

            {% if not is_incremental() and vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
            UNION
            SELECT
                address AS contract_address,
                NAME AS token_name,
                decimals AS token_decimals,
                symbol AS token_symbol,
                _inserted_timestamp,
                {{ dbt_utils.generate_surrogate_key(
                    ['address']
                ) }} AS contracts_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp,
                '{{ invocation_id }}' AS _invocation_id
            FROM
                silver.contracts_legacy -- hardcoded for ethereum, to avoid source compiling issues on other chains
            {% endif %}
                )
            SELECT
                contract_address,
                token_name,
                token_decimals,
                token_symbol,
                _inserted_timestamp,
                contracts_id,
                inserted_timestamp,
                modified_timestamp,
                _invocation_id
            FROM
                FINAL qualify(ROW_NUMBER() over(PARTITION BY contract_address
            ORDER BY
                _inserted_timestamp DESC)) = 1