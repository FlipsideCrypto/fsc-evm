{# Get variables #}
{% set vars = return_vars() %}

{# Set fact_blocks specific variables #}
{% set rpc_vars = set_dynamic_fields('fact_blocks') %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = get_path_tags(model)
) }}

SELECT
    block_number,
    block_json :hash :: STRING AS block_hash,
    utils.udf_hex_to_int(
        block_json :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    '{{ vars.GLOBAL_NETWORK_NAME }}' AS network,
    ARRAY_SIZE(
        block_json :transactions
    ) AS tx_count,
    utils.udf_hex_to_int(
        block_json :size :: STRING
    ) :: bigint AS SIZE,
    block_json :miner :: STRING AS miner,
    {% if rpc_vars.mixHash %}
    block_json :mixHash :: STRING AS mix_hash,
    {% endif %}
    block_json :extraData :: STRING AS extra_data,
    block_json :parentHash :: STRING AS parent_hash,
    utils.udf_hex_to_int(
        block_json :gasUsed :: STRING
    ) :: bigint AS gas_used,
    utils.udf_hex_to_int(
        block_json :gasLimit :: STRING
    ) :: bigint AS gas_limit,
    {% if rpc_vars.baseFeePerGas %}
    utils.udf_hex_to_int(
        block_json :baseFeePerGas :: STRING
    ) :: bigint AS base_fee_per_gas,
    {% endif %}
    utils.udf_hex_to_int(
        block_json :difficulty :: STRING
    ) :: bigint AS difficulty,
    {% if rpc_vars.totalDifficulty %}
    utils.udf_hex_to_int(
        block_json :totalDifficulty :: STRING
    ) :: bigint AS total_difficulty,
    {% endif %}
    block_json :sha3Uncles :: STRING AS sha3_uncles,
    block_json :uncles AS uncle_blocks,
    utils.udf_hex_to_int(
        block_json :nonce :: STRING
    ) :: bigint AS nonce,
    block_json :receiptsRoot :: STRING AS receipts_root,
    block_json :stateRoot :: STRING AS state_root,
    block_json :transactionsRoot :: STRING AS transactions_root,
    block_json :logsBloom :: STRING AS logs_bloom,
    {% if rpc_vars.blobGasUsed %}
    utils.udf_hex_to_int(
        block_json :blobGasUsed :: STRING
    ) :: bigint AS blob_gas_used,
    utils.udf_hex_to_int(
        block_json :excessBlobGas :: STRING
    ) :: bigint AS excess_blob_gas,
    {% endif %}
    {% if rpc_vars.parentBeaconBlockRoot %}
    block_json :parentBeaconBlockRoot :: STRING AS parent_beacon_block_root,
    {% endif %}
    {% if rpc_vars.withdrawals %}
    block_json :withdrawals AS withdrawals,
    block_json :withdrawalsRoot :: STRING AS withdrawals_root,
    {% endif %}
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