{%- set prod_network = var('GLOBAL_PROD_NETWORK', 'mainnet') -%}
{%- set uses_base_fee = var('GLOBAL_USES_BASE_FEE', true) -%}
{% set gold_full_refresh = var('GOLD_FULL_REFRESH', false) %}

{% if not gold_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = gold_full_refresh,
    tags = ['core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['core','gold']
) }}

{% endif %}

SELECT
    block_number,
    block_json :hash :: STRING AS block_hash,
    utils.udf_hex_to_int(
        block_json :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    '{{ prod_network }}' AS network,
    ARRAY_SIZE(
        block_json :transactions
    ) AS tx_count,
    utils.udf_hex_to_int(
        block_json :size :: STRING
    ) :: bigint AS SIZE,
    block_json :miner :: STRING AS miner,
    block_json :extraData :: STRING AS extra_data,
    block_json :parentHash :: STRING AS parent_hash,
    utils.udf_hex_to_int(
        block_json :gasUsed :: STRING
    ) :: bigint AS gas_used,
    utils.udf_hex_to_int(
        block_json :gasLimit :: STRING
    ) :: bigint AS gas_limit,
    {% if uses_base_fee %}
    utils.udf_hex_to_int(
        block_json :baseFeePerGas :: STRING
    ) :: bigint as  base_fee_per_gas,
    {% endif %}
    utils.udf_hex_to_int(
        block_json :difficulty :: STRING
    ) :: bigint AS difficulty,
    utils.udf_hex_to_int(
        block_json :totalDifficulty :: STRING
    ) :: bigint AS total_difficulty,
    block_json :sha3Uncles :: STRING AS sha3_uncles,
    block_json :uncles AS uncle_blocks,
    utils.udf_hex_to_int(
        block_json :nonce :: STRING
    ) :: bigint AS nonce,
    block_json :receiptsRoot :: STRING AS receipts_root,
    block_json :stateRoot :: STRING AS state_root,
    block_json :transactionsRoot :: STRING AS transactions_root,
    block_json :logsBloom :: STRING AS logs_bloom,
    {{ dbt_utils.generate_surrogate_key(['block_number']) }} AS fact_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__blocks') }}
WHERE 1=1

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
    )
{% endif %}