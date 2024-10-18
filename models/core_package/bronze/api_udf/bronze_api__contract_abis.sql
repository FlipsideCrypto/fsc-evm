-- depends_on: {{ ref('_retry_abis') }}
{% set abi_block_explorer_error_message = var(
    'ABI_BLOCK_EXPLORER_ERROR_MESSAGE',
    "abi_data :data :result :: STRING <> 'Max rate limit reached'"
) %}
{% set abi_api_interaction_count = var(
    'ABI_API_INTERACTION_COUNT',
    250
) %}
{% set abi_api_relevant_contract_limit = var(
    'ABI_API_RELEVANT_CONTRACT_LIMIT',
    5
) %}
{% set abi_api_batch_size = var(
    'ABI_API_BATCH_SIZE',
    10
) %}
{% set abi_block_explorer_url = var(
    'ABI_BLOCK_EXPLORER_URL'
) %}
{% set abi_block_explorer_secret_path = var(
    'ABI_BLOCK_EXPLORER_SECRET_PATH'
) %}
{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_contracts') }}
    WHERE
        total_interaction_count >= {{ abi_api_interaction_count }}

{% if is_incremental() %}
EXCEPT
SELECT
    contract_address
FROM
    {{ this }}
WHERE
    {{ abi_block_explorer_error_message }}
{% endif %}
LIMIT
    {{ abi_api_relevant_contract_limit }}
), all_contracts AS (
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
batched AS ({% for item in range(abi_api_batch_size) %}
SELECT
    rn.contract_address, live.udf_api('GET', CONCAT('{{ abi_block_explorer_url }}', rn.contract_address, '&apikey={key}'),{ 'User-Agent': 'FlipsideStreamline' },{}, '{{ abi_block_explorer_secret_path }}') AS abi_data, SYSDATE() AS _inserted_timestamp
FROM
    row_nos rn
WHERE
    row_no = {{ item }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    contract_address,
    abi_data,
    _inserted_timestamp
FROM
    batched
