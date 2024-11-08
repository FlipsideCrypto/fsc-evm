-- depends_on: {{ ref('_retry_abis') }}
{% set block_explorer_abi_limit = var('BLOCK_EXPLORER_ABI_LIMIT', 50) %}
{% set block_explorer_abi_url = var('BLOCK_EXPLORER_ABI_URL', '') %}
{% set block_explorer_vault_path = var('BLOCK_EXPLORER_ABI_API_KEY_PATH', '') %}
{% set block_explorer_abi_interaction_limit = var('BLOCK_EXPLORER_ABI_INTERACTION_LIMIT', 250) %}
{% set bronze_full_refresh = var('BRONZE_FULL_REFRESH', false) %}

{%- if flags.WHICH == 'compile' and execute -%}

    {{ log("=== Current Variable Settings ===", info=True) }}
    {{ log("BLOCK_EXPLORER_ABI_LIMIT: " ~ block_explorer_abi_limit, info=True) }}
    {{ log("BLOCK_EXPLORER_ABI_URL: " ~ block_explorer_abi_url, info=True) }}
    {{ log("BLOCK_EXPLORER_ABI_API_KEY_PATH: " ~ block_explorer_vault_path, info=True) }}

{%- endif -%}

{% if not bronze_full_refresh %}

{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    full_refresh = false,
    tags = ['bronze_abis']
) }}

{% else %}

{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
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
        1=1
        AND total_interaction_count > {{ block_explorer_abi_interaction_limit }}
{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
    WHERE
        1=1 and 
        abi_data:error is null
)
{% endif %}
ORDER BY
    total_interaction_count DESC
LIMIT
    {{ block_explorer_abi_limit }}    
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
batched AS ({% for item in range(block_explorer_abi_limit * 2) %}
SELECT
    rn.contract_address, 
    live.udf_api('GET', 
        CONCAT(
            '{{ block_explorer_abi_url }}', 
            rn.contract_address, 
            '&apikey={key}'
        ),
        {'User-Agent': 'FlipsideStreamline'},
        {},
        '{{ block_explorer_vault_path }}'
    ) AS abi_data
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
    SYSDATE() AS _inserted_timestamp
FROM
    batched
