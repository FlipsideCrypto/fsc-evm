{% set prod_network = var('GLOBAL_PROD_NETWORK', 'mainnet') %}

{# Prod DB Variables Start #}
{# Query RPC settings for current chain #}
{% set rpc_settings_query %}
  select 
    BLOCKS_HAS_BASE_FEE,
    BLOCKS_HAS_TOTAL_DIFFICULTY,
    BLOCKS_HAS_MIX_HASH,
    BLOCKS_HAS_BLOB_GAS_USED,
    BLOCKS_HAS_PARENT_BEACON_BLOCK_ROOT,
    BLOCKS_HAS_WITHDRAWALS
  from {{ target.database }}.utils.rpc_settings
{% endset %}

{% set results = run_query(rpc_settings_query) %}

{% if execute %}
  {% set row = results.rows[0] %}
  {% set uses_base_fee = row['BLOCKS_HAS_BASE_FEE'] %}
  {% set uses_total_difficulty = row['BLOCKS_HAS_TOTAL_DIFFICULTY'] %}
  {% set uses_mix_hash = row['BLOCKS_HAS_MIX_HASH'] %}
  {% set uses_blob_gas_used = row['BLOCKS_HAS_BLOB_GAS_USED'] %}
  {% set uses_parent_beacon_block_root = row['BLOCKS_HAS_PARENT_BEACON_BLOCK_ROOT'] %}
  {% set uses_withdrawals = row['BLOCKS_HAS_WITHDRAWALS'] %}
  
  {# Debug logging #}
  {{ log("Feature flags:", info=True) }}
  {{ log("uses_base_fee: " ~ uses_base_fee, info=True) }}
  {{ log("uses_total_difficulty: " ~ uses_total_difficulty, info=True) }}
  {{ log("uses_mix_hash: " ~ uses_mix_hash, info=True) }}
{% endif %}
{# Prod DB Variables End #}

{% set gold_full_refresh = var('GOLD_FULL_REFRESH', false) %}

{# Log configuration details #}
{{ log_model_details() }}

{% if not gold_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = gold_full_refresh,
    tags = ['gold_core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['gold_core']
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
    {% if uses_mix_hash %}
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
    {% if uses_base_fee %}
    utils.udf_hex_to_int(
        block_json :baseFeePerGas :: STRING
    ) :: bigint AS base_fee_per_gas,
    {% endif %}
    utils.udf_hex_to_int(
        block_json :difficulty :: STRING
    ) :: bigint AS difficulty,
    {% if uses_total_difficulty %}
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
    {% if uses_blob_gas_used %}
    utils.udf_hex_to_int(
        block_json :blobGasUsed :: STRING
    ) :: bigint AS blob_gas_used,
    utils.udf_hex_to_int(
        block_json :excessBlobGas :: STRING
    ) :: bigint AS excess_blob_gas,
    {% endif %}
    {% if uses_parent_beacon_block_root %}
    block_json :parentBeaconBlockRoot :: STRING AS parent_beacon_block_root,
    {% endif %}
    {% if uses_withdrawals %}
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