{% set api_abi_error_message = var(
    'API_ABI_ERROR_MESSAGE',
    "abi_data :data :result :: STRING <> 'Max rate limit reached'"
) %}
{% set api_abi_interaction_count = var(
    'API_ABI_INTERACTION_COUNT',
    250
) %}
{% set api_abi_relevant_contract_limit = var(
    'API_ABI_RELEVANT_CONTRACT_LIMIT',
    5
) %}
{% set api_abi_batch_size = var(
    'API_ABI_BATCH_SIZE',
    10
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
        total_interaction_count >= {{ api_abi_interaction_count }}

{% if is_incremental() %}
EXCEPT
SELECT
    contract_address
FROM
    {{ this }}
WHERE
    '{{ api_abi_error_message }}'
{% endif %}
LIMIT
    {{ api_abi_relevant_contract_limit }}
), all_contracts AS (
    SELECT
        contract_address
    FROM
        base
    UNION
    SELECT
        contract_address
    FROM
        {{ ref('_retry_abis') }}
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
batched AS ({% for item in range({{ api_abi_batch_size }}) %}
SELECT
    rn.contract_address, live.udf_api('GET', CONCAT('{{GLOBAL_API_URL}}', rn.contract_address, '&apikey=', '{{TEMP_ABI_KEY}}'),{ 'User-Agent': 'FlipsideStreamline' },{},{}) AS abi_data, SYSDATE() AS _inserted_timestamp
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
