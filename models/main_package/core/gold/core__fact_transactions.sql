{# Get variables #}
{% set vars = return_vars() %}

{# Set fact_transactions specific variables #}
{% set rpc_vars = set_dynamic_fields('fact_transactions') %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = vars.MAIN_CORE_GOLD_FACT_TRANSACTIONS_UNIQUE_KEY,
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_number,tx_hash,from_address,to_address,origin_function_signature), SUBSTRING(input_data)",
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','core','phase_2']
) }}

WITH base AS (

    SELECT
        block_number,
        tx_position,
        transaction_json
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }})
        {% endif %}
    ),
    transactions_fields AS (
        SELECT
            block_number,
            tx_position,
            transaction_json :blockHash :: STRING AS block_hash,
            transaction_json :blockNumber :: STRING AS block_number_hex,
            transaction_json :from :: STRING AS from_address,
            utils.udf_hex_to_int(
                transaction_json :gas :: STRING
            ) :: bigint AS gas_limit,
            TRY_TO_NUMBER(utils.udf_hex_to_int(
                transaction_json :gasPrice :: STRING
            )) AS gas_price,
            transaction_json :hash :: STRING AS tx_hash,
            transaction_json :input :: STRING AS input_data,
            LEFT(
                input_data,
                10
            ) AS origin_function_signature,
            {% if rpc_vars.mint %}
            utils.udf_hex_to_int(
                transaction_json :mint :: STRING
            ) AS mint_precise_raw,
            utils.udf_decimal_adjust(
                mint_precise_raw,
                18
            ) AS mint_precise,
            mint_precise :: FLOAT AS mint,
            {% endif %}
            utils.udf_hex_to_int(
                transaction_json :nonce :: STRING
            ) :: bigint AS nonce,
            transaction_json :r :: STRING AS r,
            transaction_json :s :: STRING AS s,
            {% if rpc_vars.sourceHash %}
            transaction_json :sourceHash :: STRING AS source_hash,
            {% endif %}
            transaction_json :to :: STRING AS to_address1,
            CASE
                WHEN to_address1 = '' THEN NULL
                ELSE to_address1
            END AS to_address,
            utils.udf_hex_to_int(
                transaction_json :transactionIndex :: STRING
            ) :: bigint AS transaction_index,
            utils.udf_hex_to_int(
                transaction_json :type :: STRING
            ) :: bigint AS tx_type,
            utils.udf_hex_to_int(
                transaction_json :v :: STRING
            ) :: bigint AS v,
            {% if rpc_vars.maxFeePerGas %}
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_fee_per_gas,
            {% endif %}
            {% if rpc_vars.maxPriorityFeePerGas %}
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxPriorityFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_priority_fee_per_gas,
            {% endif %}
            {% if rpc_vars.blobVersionedHashes %}
            transaction_json :blobVersionedHashes AS blob_versioned_hashes,
            {% endif %}
            {% if rpc_vars.maxFeePerBlobGas %}
            utils.udf_hex_to_int(transaction_json :maxFeePerBlobGas :: STRING) / pow(10, 9) AS max_fee_per_blob_gas,   
            {% endif %}
            {% if rpc_vars.ethValue %}
            utils.udf_hex_to_int(
                transaction_json :ethValue :: STRING
            ) AS eth_value_precise_raw,
            utils.udf_decimal_adjust(
                eth_value_precise_raw,
                18
            ) AS eth_value_precise,
            eth_value_precise :: FLOAT AS eth_value,
            {% endif %}
            utils.udf_hex_to_int(
                transaction_json :value :: STRING
            ) AS value_precise_raw,
            utils.udf_decimal_adjust(
                value_precise_raw,
                18
            ) AS value_precise,
            value_precise :: FLOAT AS VALUE,
            {% if rpc_vars.yParity %}
            utils.udf_hex_to_int(transaction_json :yParity :: STRING):: bigint AS y_parity,
            {% endif %}
            {% if rpc_vars.accessList %}
            transaction_json :accessList AS access_list,
            {% endif %}
        FROM
            base
    ),
    new_transactions AS (
        SELECT
            txs.block_number,
            txs.block_hash,
            b.block_timestamp,
            txs.tx_hash,
            txs.from_address,
            txs.to_address,
            txs.origin_function_signature,
            {% if rpc_vars.mint %}
            txs.mint,
            txs.mint_precise_raw,
            txs.mint_precise,
            {% endif %}
            {% if rpc_vars.ethValue %}
            txs.eth_value,
            txs.eth_value_precise_raw,
            txs.eth_value_precise,
            {% endif %}
            txs.value,
            txs.value_precise_raw,
            txs.value_precise,
            {% if rpc_vars.maxFeePerGas %}
            txs.max_fee_per_gas,
            {% endif %}
            {% if rpc_vars.maxPriorityFeePerGas %}
            txs.max_priority_fee_per_gas,
            {% endif %}
            {% if rpc_vars.blobVersionedHashes %}
            txs.blob_versioned_hashes,
            {% endif %}
            {% if rpc_vars.maxFeePerBlobGas %}
            txs.max_fee_per_blob_gas,   
            {% endif %}
            {% if rpc_vars.blobGasPrice %}
            utils.udf_hex_to_int(r.receipts_json :blobGasPrice :: STRING) / pow(10, 9) as blob_gas_price,
            {% endif %}
            {% if rpc_vars.blobGasUsed %}
            utils.udf_hex_to_int(r.receipts_json :blobGasUsed :: STRING) as blob_gas_used,
            {% endif %}
            {% if rpc_vars.l1Fee %}
            utils.udf_hex_to_int(r.receipts_json :l1Fee :: STRING) as l1_fee_precise_raw,
            utils.udf_decimal_adjust(l1_fee_precise_raw, 18) as l1_fee_precise,
            l1_fee_precise :: FLOAT AS l1_fee,
            {% endif %}
            {% if rpc_vars.l1FeeScalar %}
            COALESCE(
                (
                    r.receipts_json :l1FeeScalar :: STRING
                ) :: FLOAT,
                0
            ) AS l1_fee_scalar,
            {% endif %}
            {% if rpc_vars.l1GasUsed %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1GasUsed :: STRING
                ) :: BIGINT,
                0
            ) AS l1_gas_used,
            {% endif %}
            {% if rpc_vars.l1GasPrice %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1GasPrice :: STRING
                ) :: BIGINT,
                0
            ) AS l1_gas_price,
            {% endif %}
            {% if rpc_vars.l1BaseFeeScalar %}
            utils.udf_hex_to_int(r.receipts_json :l1BaseFeeScalar :: STRING):: bigint AS l1_base_fee_scalar,
            {% endif %}
            {% if rpc_vars.gasUsedForL1 %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :gasUsedForL1 :: STRING
                ) :: bigint,
                0
            ) AS gas_used_for_l1,
            {% endif %}
            {% if rpc_vars.l1BlockNumber %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1BlockNumber :: STRING
                ) :: bigint,
                0
            ) AS l1_block_number,
            {% endif %}
            {% if rpc_vars.yParity %}
            txs.y_parity,
            {% endif %}
            {% if rpc_vars.accessList %}
            txs.access_list,
            {% endif %}
            {% if rpc_vars.tokenRatio %}
            TRY_TO_NUMBER(utils.udf_hex_to_int(r.receipts_json :tokenRatio :: STRING)) AS token_ratio,
            {% endif %}
            {% if rpc_vars.l1BlobBaseFee %}
            utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFee :: STRING):: bigint AS l1_blob_base_fee,
            {% endif %}
            {% if rpc_vars.l1BlobBaseFeeScalar %}
            utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFeeScalar :: STRING):: bigint AS l1_blob_base_fee_scalar,
            {% endif %}
             {% if rpc_vars.operatorFeeScalar %}
                utils.udf_hex_to_int(
                    r.receipts_json :operatorFeeScalar :: STRING
                ) :: bigint AS operator_fee_scalar,
            {% endif %}
            {% if rpc_vars.operatorFeeConstant %}
                utils.udf_hex_to_int(
                    r.receipts_json :operatorFeeConstant :: STRING
                ) :: bigint AS operator_fee_constant,
            {% endif %}
            txs.gas_price / pow(
                10,
                9
            ) AS gas_price,
            utils.udf_hex_to_int(
                r.receipts_json :gasUsed :: STRING
            ) :: bigint AS gas_used,
            txs.gas_limit,
            utils.udf_hex_to_int(
                r.receipts_json :cumulativeGasUsed :: STRING
            ) :: bigint AS cumulative_gas_used,
            utils.udf_hex_to_int(
                r.receipts_json :effectiveGasPrice :: STRING
            ) :: bigint AS effective_gas_price,
            {% if rpc_vars.l1Fee %}
            utils.udf_decimal_adjust(
                (
                    txs.gas_price * utils.udf_hex_to_int(
                        r.receipts_json :gasUsed :: STRING
                    ) :: bigint
                ) + ifnull(l1_fee_precise_raw :: bigint,0)
                {% if rpc_vars.operatorFeeScalar or rpc_vars.operatorFeeConstant %}
                 + (
                (
                    utils.udf_hex_to_int(r.receipts_json :gasUsed :: STRING) :: bigint 
                 * COALESCE(operator_fee_scalar, 0) / pow(10, 6)
                 ) 
                 + COALESCE(operator_fee_constant,0)
                    )
                    {% endif %}
                ,18
            ) AS tx_fee_precise,
            {% elif vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
            utils.udf_decimal_adjust(
                effective_gas_price * utils.udf_hex_to_int(
                    r.receipts_json :gasUsed :: STRING
                ) :: bigint,
                18
            ) AS tx_fee_precise,
            {% else %}
            utils.udf_decimal_adjust(
                txs.gas_price * utils.udf_hex_to_int(
                    r.receipts_json :gasUsed :: STRING
                ) :: bigint,
                18
            ) AS tx_fee_precise,
            {% endif %}
            COALESCE(
                tx_fee_precise :: FLOAT,
                0
            ) AS tx_fee,
            CASE
                WHEN r.receipts_json :status :: STRING = '0x1' THEN TRUE
                WHEN r.receipts_json :status :: STRING = '0x0' THEN FALSE
                ELSE NULL
            END AS tx_succeeded,
            txs.tx_type,
            {% if rpc_vars.timeboosted %}
                r.receipts_json :timeboosted :: BOOLEAN AS timeboosted,
            {% endif %}
            txs.nonce,
            txs.tx_position,
            txs.input_data,
            txs.r,
            txs.s,
            {% if rpc_vars.sourceHash %}
            txs.source_hash,
            {% endif %}
            txs.v
        FROM
            transactions_fields txs
            LEFT JOIN {{ ref('core__fact_blocks') }}
            b
            ON txs.block_number = b.block_number

{% if is_incremental() %}
AND b.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
LEFT JOIN {{ ref('silver__receipts') }}
r
ON txs.block_number = r.block_number
AND txs.tx_hash =
{% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
    r.tx_hash
{% else %}
    r.receipts_json :transactionHash :: STRING
{% endif %}

{% if is_incremental() %}
AND r.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp AS block_timestamp_heal,
        t.tx_hash,
        t.from_address,
        t.to_address,
        t.origin_function_signature,
        {% if rpc_vars.mint %}
        t.mint,
        t.mint_precise_raw,
        t.mint_precise,
        {% endif %}
        {% if rpc_vars.ethValue %}
        t.eth_value,
        t.eth_value_precise_raw,
        t.eth_value_precise,
        {% endif %}
        t.value,
        t.value_precise_raw,
        t.value_precise,
        {% if rpc_vars.maxFeePerGas %}
        t.max_fee_per_gas,
        {% endif %}
        {% if rpc_vars.maxPriorityFeePerGas %}
        t.max_priority_fee_per_gas,
        {% endif %}
        {% if rpc_vars.blobVersionedHashes %}
        t.blob_versioned_hashes,
        {% endif %}
        {% if rpc_vars.maxFeePerBlobGas %}
        t.max_fee_per_blob_gas,   
        {% endif %}
        {% if rpc_vars.blobGasPrice %}
        utils.udf_hex_to_int(r.receipts_json :blobGasPrice :: STRING) / pow(10, 9) as blob_gas_price_heal,
        {% endif %}
        {% if rpc_vars.blobGasUsed %}
        utils.udf_hex_to_int(r.receipts_json :blobGasUsed :: STRING) as blob_gas_used_heal,
        {% endif %}
        {% if rpc_vars.l1Fee %}
        utils.udf_hex_to_int(r.receipts_json :l1Fee :: STRING) as l1_fee_precise_raw_heal,
        utils.udf_decimal_adjust(l1_fee_precise_raw_heal, 18) as l1_fee_precise_heal,
        l1_fee_precise_heal :: FLOAT AS l1_fee_heal,
        {% endif %}
        {% if rpc_vars.l1FeeScalar %}
        COALESCE(
            (
                r.receipts_json :l1FeeScalar :: STRING
            ) :: FLOAT,
            0
        ) AS l1_fee_scalar_heal,
        {% endif %}
        {% if rpc_vars.l1GasUsed %}
        COALESCE(
            utils.udf_hex_to_int(
                r.receipts_json :l1GasUsed :: STRING
            ) :: BIGINT,
            0
        ) AS l1_gas_used_heal,
        {% endif %}
        {% if rpc_vars.l1GasPrice %}
        COALESCE(
            utils.udf_hex_to_int(
                r.receipts_json :l1GasPrice :: STRING
            ) :: BIGINT,
            0
        ) AS l1_gas_price_heal,
        {% endif %}
        {% if rpc_vars.l1BaseFeeScalar %}
        utils.udf_hex_to_int(r.receipts_json :l1BaseFeeScalar :: STRING):: bigint AS l1_base_fee_scalar_heal,
        {% endif %}
        {% if rpc_vars.gasUsedForL1 %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :gasUsedForL1 :: STRING
                ) :: bigint,
                0
            ) AS gas_used_for_l1_heal,
            {% endif %}
            {% if rpc_vars.l1BlockNumber %}
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1BlockNumber :: STRING
                ) :: bigint,
                0
            ) AS l1_block_number_heal,
        {% endif %}
        {% if rpc_vars.yParity %}
        t.y_parity,
        {% endif %}
        {% if rpc_vars.accessList %}
        t.access_list,
        {% endif %}
        {% if rpc_vars.tokenRatio %}
        TRY_TO_NUMBER(utils.udf_hex_to_int(r.receipts_json :tokenRatio :: STRING)) AS token_ratio_heal,
        {% endif %}
        {% if rpc_vars.l1BlobBaseFee %}
        utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFee :: STRING):: bigint AS l1_blob_base_fee_heal,
        {% endif %}
        {% if rpc_vars.l1BlobBaseFeeScalar %}
        utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFeeScalar :: STRING):: bigint AS l1_blob_base_fee_scalar_heal,
        {% endif %}
        {% if rpc_vars.operatorFeeScalar %}
            utils.udf_hex_to_int(
                r.receipts_json :operatorFeeScalar :: STRING
            ) :: bigint AS operator_fee_scalar_heal,
        {% endif %}
        {% if rpc_vars.operatorFeeConstant %}
            utils.udf_hex_to_int(
                r.receipts_json :operatorFeeConstant :: STRING
            ) :: bigint AS operator_fee_constant_heal,
        {% endif %}
        {% if vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
        t.gas_price_bid as gas_price, 
        {% else %}
        t.gas_price,
        {% endif %}
        utils.udf_hex_to_int(
            r.receipts_json :gasUsed :: STRING
        ) :: bigint AS gas_used_heal,
        t.gas_limit,
        utils.udf_hex_to_int(
            r.receipts_json :cumulativeGasUsed :: STRING
        ) :: bigint AS cumulative_gas_used_heal,
        utils.udf_hex_to_int(
            r.receipts_json :effectiveGasPrice :: STRING
        ) :: bigint AS effective_gas_price_heal,
        {% if rpc_vars.l1Fee %}
            utils.udf_decimal_adjust(
                (
                    (t.gas_price * pow(10, 9)) * utils.udf_hex_to_int(
                        r.receipts_json :gasUsed :: STRING
                    ) :: bigint
                ) + ifnull(l1_fee_precise_raw_heal :: bigint,0)
                {% if rpc_vars.operatorFeeScalar or rpc_vars.operatorFeeConstant %}
                 + (
                (
                    utils.udf_hex_to_int(r.receipts_json :gasUsed :: STRING) :: bigint 
                 * COALESCE(operator_fee_scalar_heal, 0) / pow(10, 6)
                 ) 
                 + COALESCE(operator_fee_constant_heal,0)
                    )
                    {% endif %}
                ,18
            ) AS tx_fee_precise_heal,
        {% elif vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
            utils.udf_decimal_adjust(
                effective_gas_price_heal * utils.udf_hex_to_int(
                    r.receipts_json :gasUsed :: STRING
                ) :: bigint,
                18
            ) AS tx_fee_precise_heal,
        {% else %}
        utils.udf_decimal_adjust(
            t.gas_price * utils.udf_hex_to_int(
                r.receipts_json :gasUsed :: STRING
            ) :: bigint, 
            9
        ) AS tx_fee_precise_heal,
        {% endif %}
        COALESCE(
            tx_fee_precise_heal :: FLOAT,
            0
        ) AS tx_fee_heal,
        CASE
            WHEN r.receipts_json :status :: STRING = '0x1' THEN TRUE
            WHEN r.receipts_json :status :: STRING = '0x0' THEN FALSE
            ELSE NULL
        END AS tx_succeeded_heal,
        t.tx_type,
        {% if rpc_vars.timeboosted %}
            r.receipts_json :timeboosted :: BOOLEAN AS timeboosted_heal,
        {% endif %}
        t.nonce,
        t.tx_position,
        t.input_data,
        t.r,
        t.s,
        {% if rpc_vars.sourceHash %}
        t.source_hash,
        {% endif %}
        t.v
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core__fact_blocks') }}
        b
        ON t.block_number = b.block_number
        LEFT JOIN {{ ref('silver__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash =
        {% if vars.MAIN_CORE_RECEIPTS_BY_HASH_ENABLED %}
            r.tx_hash
        {% else %}
            r.receipts_json :transactionHash :: STRING
        {% endif %}
    WHERE
        t.block_timestamp IS NULL
        OR t.tx_succeeded IS NULL
)
{% endif %},
all_transactions AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        origin_function_signature,
        {% if rpc_vars.mint %}
        mint,
        mint_precise_raw,
        mint_precise,
        {% endif %}
        {% if rpc_vars.ethValue %}
        eth_value,
        eth_value_precise_raw,
        eth_value_precise,
        {% endif %}
        VALUE,
        value_precise_raw,
        value_precise,
        {% if rpc_vars.maxFeePerGas %}
        max_fee_per_gas,
        {% endif %}
        {% if rpc_vars.maxPriorityFeePerGas %}
        max_priority_fee_per_gas,
        {% endif %}
        {% if rpc_vars.blobVersionedHashes %}
        blob_versioned_hashes,
        {% endif %}
        {% if rpc_vars.maxFeePerBlobGas %}
        max_fee_per_blob_gas,   
        {% endif %}
        {% if rpc_vars.blobGasUsed %}
        blob_gas_used,
        {% endif %}
        {% if rpc_vars.blobGasPrice %}
        blob_gas_price,
        {% endif %}
        {% if rpc_vars.l1Fee %}
        l1_fee,
        l1_fee_precise_raw,
        l1_fee_precise,
        {% endif %}
        {% if rpc_vars.l1FeeScalar %}
        l1_fee_scalar,
        {% endif %}
        {% if rpc_vars.l1GasUsed %}
        l1_gas_used,
        {% endif %}
        {% if rpc_vars.l1GasPrice %}
        l1_gas_price / pow(10, 9) as l1_gas_price,
        {% endif %}
        {% if rpc_vars.l1BaseFeeScalar %}
        l1_base_fee_scalar,
        {% endif %}
        {% if rpc_vars.gasUsedForL1 %}
        gas_used_for_l1,
        {% endif %}
        {% if rpc_vars.l1BlockNumber %}
        l1_block_number,
        {% endif %}
        {% if rpc_vars.yParity %}
        y_parity,
        {% endif %}
        {% if rpc_vars.accessList %}
        access_list,
        {% endif %}
        {% if rpc_vars.tokenRatio %}
        token_ratio,
        {% endif %}
        {% if rpc_vars.l1BlobBaseFee %}
        l1_blob_base_fee,
        {% endif %}
        {% if rpc_vars.l1BlobBaseFeeScalar %}
        l1_blob_base_fee_scalar,
        {% endif %}
        {% if rpc_vars.operatorFeeScalar %}
        operator_fee_scalar,
        {% endif %}
        {% if rpc_vars.operatorFeeConstant %}
        operator_fee_constant,
        {% endif %}
        tx_fee,
        tx_fee_precise,
        tx_succeeded,
        tx_type,
        {% if rpc_vars.timeboosted %}
        timeboosted,
        {% endif %}
        nonce,
        tx_position,
        input_data,
        gas_price,
        gas_used,
        gas_limit,
        cumulative_gas_used,
        effective_gas_price / pow(10, 9) as effective_gas_price,
        r,
        s,
        {% if rpc_vars.sourceHash %}
        source_hash,
        {% endif %}
        v
    FROM
        new_transactions

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    {% if rpc_vars.mint %}
    mint,
    mint_precise_raw,
    mint_precise,
    {% endif %}
    {% if rpc_vars.ethValue %}
    eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    {% endif %}
    VALUE,
    value_precise_raw,
    value_precise,
    {% if rpc_vars.maxFeePerGas %}
    max_fee_per_gas,
    {% endif %}
    {% if rpc_vars.maxPriorityFeePerGas %}
    max_priority_fee_per_gas,
    {% endif %}
    {% if rpc_vars.blobVersionedHashes %}
    blob_versioned_hashes,
    {% endif %}
    {% if rpc_vars.maxFeePerBlobGas %}
    max_fee_per_blob_gas,   
    {% endif %}
    {% if rpc_vars.blobGasUsed %}
    blob_gas_used_heal AS blob_gas_used,
    {% endif %}
    {% if rpc_vars.blobGasPrice %}
    blob_gas_price_heal AS blob_gas_price,
    {% endif %}
    {% if rpc_vars.l1Fee %}
    l1_fee_precise_heal AS l1_fee,
    l1_fee_precise_raw_heal AS l1_fee_precise_raw,
    l1_fee_precise_heal AS l1_fee_precise,
    {% endif %}
    {% if rpc_vars.l1FeeScalar %}
    l1_fee_scalar_heal AS l1_fee_scalar,
    {% endif %}
    {% if rpc_vars.l1GasUsed %}
    l1_gas_used_heal AS l1_gas_used,
    {% endif %}
    {% if rpc_vars.l1GasPrice %}
    l1_gas_price_heal / pow(10, 9) AS l1_gas_price,
    {% endif %}
    {% if rpc_vars.l1BaseFeeScalar %}
    l1_base_fee_scalar_heal AS l1_base_fee_scalar,
    {% endif %}
    {% if rpc_vars.gasUsedForL1 %}
    gas_used_for_l1_heal AS gas_used_for_l1,
    {% endif %}
    {% if rpc_vars.l1BlockNumber %}
    l1_block_number_heal AS l1_block_number,
    {% endif %}
    {% if rpc_vars.yParity %}
    y_parity,
    {% endif %}
    {% if rpc_vars.accessList %}
    access_list,
    {% endif %}
    {% if rpc_vars.tokenRatio %}
    token_ratio_heal AS token_ratio,
    {% endif %}
    {% if rpc_vars.l1BlobBaseFee %}
    l1_blob_base_fee_heal AS l1_blob_base_fee,
    {% endif %}
    {% if rpc_vars.l1BlobBaseFeeScalar %}
    l1_blob_base_fee_scalar_heal AS l1_blob_base_fee_scalar,
    {% endif %}
    {% if rpc_vars.operatorFeeScalar %}
    operator_fee_scalar_heal AS operator_fee_scalar,
    {% endif %}
    {% if rpc_vars.operatorFeeConstant %}
    operator_fee_constant_heal AS operator_fee_constant,
    {% endif %}
    tx_fee_heal AS tx_fee,
    tx_fee_precise_heal AS tx_fee_precise,
    tx_succeeded_heal AS tx_succeeded,
    tx_type,
    {% if rpc_vars.timeboosted %}
    timeboosted_heal AS timeboosted,
    {% endif %}
    nonce,
    tx_position,
    input_data,
    gas_price,
    gas_used_heal AS gas_used,
    gas_limit,
    cumulative_gas_used_heal AS cumulative_gas_used,
    effective_gas_price_heal / pow(10, 9) AS effective_gas_price,
    r,
    s,
    {% if rpc_vars.sourceHash %}
    source_hash,
    {% endif %}
    v
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    COALESCE(tx_fee_precise,'0') AS tx_fee_precise,
    tx_succeeded,
    tx_type,
    {% if rpc_vars.timeboosted %}
    timeboosted,
    {% endif %}
    nonce,
    tx_position,
    input_data,
    {% if vars.GLOBAL_PROJECT_NAME == 'arbitrum' %}
    gas_price as gas_price_bid, 
    effective_gas_price as gas_price_paid,
    {% else %}
    gas_price,
    effective_gas_price,
    {% endif %}
    gas_used,
    gas_limit,
    cumulative_gas_used,
    {% if rpc_vars.maxFeePerGas %}
    max_fee_per_gas,
    {% endif %}
    {% if rpc_vars.maxPriorityFeePerGas %}
    max_priority_fee_per_gas,
    {% endif %}
    {% if rpc_vars.blobVersionedHashes %}
    blob_versioned_hashes,
    {% endif %}
    {% if rpc_vars.maxFeePerBlobGas %}
    max_fee_per_blob_gas,   
    {% endif %}
    {% if rpc_vars.blobGasUsed %}
    blob_gas_used,
    {% endif %}
    {% if rpc_vars.blobGasPrice %}
    blob_gas_price,
    {% endif %}
    {% if rpc_vars.l1Fee %}
    l1_fee,
    l1_fee_precise_raw,
    l1_fee_precise,
    {% endif %}
    {% if rpc_vars.l1FeeScalar %}
    l1_fee_scalar,
    {% endif %}
    {% if rpc_vars.l1GasUsed %}
    l1_gas_used,
    {% endif %}
    {% if rpc_vars.l1GasPrice %}
    l1_gas_price,
    {% endif %}
    {% if rpc_vars.l1BaseFeeScalar %}
    l1_base_fee_scalar,
    {% endif %}
    {% if rpc_vars.l1BlobBaseFee %}
    l1_blob_base_fee,
    {% endif %}
    {% if rpc_vars.l1BlobBaseFeeScalar %}
    l1_blob_base_fee_scalar,
    {% endif %}
    {% if rpc_vars.operatorFeeScalar %}
    operator_fee_scalar,
    {% endif %}
    {% if rpc_vars.operatorFeeConstant %}
    operator_fee_constant,
    {% endif %}
    {% if rpc_vars.mint %}
    mint,
    mint_precise_raw,
    mint_precise,
    {% endif %}
    {% if rpc_vars.ethValue %}
    eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    {% endif %}
    {% if rpc_vars.l1BlockNumber %}
    l1_block_number,
    {% endif %}
    {% if rpc_vars.gasUsedForL1 %}
    gas_used_for_l1,
    {% endif %}
    {% if rpc_vars.yParity %}
    y_parity,
    {% endif %}
    {% if rpc_vars.accessList %}
    access_list,
    {% endif %}
    {% if rpc_vars.tokenRatio %}
    token_ratio,
    {% endif %}
    r,
    s,
    v,
    {% if rpc_vars.sourceHash %}
    source_hash,
    {% endif %}
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS fact_transactions_id,
    {% if is_incremental() or vars.GLOBAL_NEW_BUILD_ENABLED %}
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
    {% else %}
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS inserted_timestamp,
    CASE WHEN block_timestamp >= date_trunc('hour',SYSDATE()) - interval '6 hours' THEN SYSDATE() 
        ELSE GREATEST(block_timestamp, dateadd('day', -10, SYSDATE())) END AS modified_timestamp
    {% endif %}
FROM
    all_transactions qualify ROW_NUMBER() over (
        PARTITION BY fact_transactions_id
        ORDER BY
            block_number DESC,
            block_timestamp DESC nulls last,
            tx_succeeded DESC nulls last
    ) = 1