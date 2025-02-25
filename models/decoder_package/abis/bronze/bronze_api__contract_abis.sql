{% set block_explorer_abi_limit = get_var('DECODER_ABIS_EXPLORER_LIMIT', 50) %}
{% set block_explorer_abi_url = get_var('DECODER_ABIS_EXPLORER_URL', '') %}
{% set block_explorer_abi_url_suffix = get_var('DECODER_ABIS_EXPLORER_URL_SUFFIX', '') %}
{% set block_explorer_vault_path = get_var('DECODER_ABIS_EXPLORER_API_KEY_VAULT_PATH', '') %}
{% set block_explorer_abi_interaction_limit = get_var('DECODER_ABIS_EXPLORER_INTERACTION_LIMIT', 250) %}
{% set bronze_full_refresh = get_var('DECODER_ABIS_CONTRACT_ABIS_FR_ENABLED', false) %}


{# Log configuration details #}
{{ log_model_details() }}

{% if not bronze_full_refresh %}
-- depends_on: {{ ref('_retry_abis') }}
{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    full_refresh = false,
    tags = ['bronze_abis', 'phase_2']
) }}

{% else %}

{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['bronze_abis', 'phase_2']
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
        AND total_interaction_count > {{ block_explorer_abi_interaction_limit }}

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
    {{ block_explorer_abi_limit }}
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
            block_explorer_abi_limit * 2
        ) %}
    SELECT
        rn.contract_address,
        live.udf_api('GET',
            CONCAT(
                '{{ block_explorer_abi_url }}',
                rn.contract_address
                {% if block_explorer_vault_path != '' %}
                ,'&apikey={key}'
                {% endif %}
                {% if block_explorer_abi_url_suffix != '' %}
                ,'{{ block_explorer_abi_url_suffix }}'
                {% endif %}
            ),
            {'User-Agent': 'FlipsideStreamline'},
            null,
            '{{ block_explorer_vault_path }}'
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
