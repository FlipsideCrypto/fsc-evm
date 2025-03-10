{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('_retry_abis') }}

{% if vars.GLOBAL_BRONZE_FR_ENABLED %}
{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['bronze_abis']
) }}
{% else %}
{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    full_refresh = vars.GLOBAL_BRONZE_FR_ENABLED,
    tags = ['bronze_abis']
) }}
{% endif %}

WITH base AS (

    SELECT
        contract_address,
        total_interaction_count
    FROM
        {{ ref('silver__relevant_contracts') }}
    WHERE
        1 = 1
        AND total_interaction_count > {{ vars.DECODER_ABIS_EXPLORER_INTERACTION_LIMIT }}

{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
    WHERE
        1 = 1
        AND abi_data :error IS NULL
)
{% endif %}
ORDER BY
    total_event_count DESC
LIMIT
    {{ vars.DECODER_ABIS_EXPLORER_LIMIT }}
), 
all_contracts AS (
    SELECT
        contract_address
    FROM
        base

{% if is_incremental() %}
UNION
SELECT
    contract_address
FROM
    {{ ref('_retry_abis') }}
{% endif %}
),
row_nos AS (
    SELECT
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no
    FROM
        all_contracts
),
batched AS (
    {% for item in range(
            vars.DECODER_ABIS_EXPLORER_LIMIT * 2
        ) %}
    SELECT
        rn.contract_address,
        live.udf_api('GET',
            CONCAT(
                '{{ vars.DECODER_ABIS_EXPLORER_URL }}',
                rn.contract_address
                {% if vars.DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH != '' %}
                ,'&apikey={key}'
                {% endif %}
                {% if vars.DECODER_ABIS_EXPLORER_URL_SUFFIX != '' %}
                ,'{{ vars.DECODER_ABIS_EXPLORER_URL_SUFFIX }}'
                {% endif %}
            ),
            OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'livequery'
            ),
            NULL,
            '{{ vars.DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH }}'
        ) AS abi_data
    FROM
        row_nos rn
    WHERE
        row_no = {{ item }}

        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    contract_address,
    abi_data,
    SYSDATE() AS _inserted_timestamp
FROM
    batched
