
{# Prod DB Variables Start #}
{# Query RPC settings for current chain #}
{% set rpc_settings_query %}
    select 
        TX_HAS_ACCESS_LIST,
        TX_HAS_MAX_FEE_PER_GAS,
        TX_HAS_MAX_PRIORITY_FEE_PER_GAS,
        TX_HAS_BLOB_GAS_PRICE,
        TX_HAS_SOURCE_HASH,
        TX_HAS_MINT,
        TX_HAS_ETH_VALUE,
        TX_HAS_Y_PARITY,
        TX_HAS_L1_COLUMNS,
        TX_HAS_L1_TX_FEE_CALC,
        TX_HAS_BLOB_BASE_FEE,
        TX_HAS_EIP_1559
    from {{ target.database }}.utils.rpc_settings
{% endset %}

{% set results = run_query(rpc_settings_query) %}

{% if execute %}
    {% set row = results.rows[0] %}
    {% set uses_access_list = row['TX_HAS_ACCESS_LIST'] %}
    {% set uses_max_fee_per_gas = row['TX_HAS_MAX_FEE_PER_GAS'] %}
    {% set uses_max_priority_fee_per_gas = row['TX_HAS_MAX_PRIORITY_FEE_PER_GAS'] %}
    {% set uses_blob_gas_price = row['TX_HAS_BLOB_GAS_PRICE'] %}
    {% set uses_source_hash = row['TX_HAS_SOURCE_HASH'] %}
    {% set uses_mint = row['TX_HAS_MINT'] %}
    {% set uses_eth_value = row['TX_HAS_ETH_VALUE'] %}
    {% set uses_y_parity = row['TX_HAS_Y_PARITY'] %}
    {% set uses_l1_columns = row['TX_HAS_L1_COLUMNS'] %}
    {% set uses_l1_tx_fee_calc = row['TX_HAS_L1_TX_FEE_CALC'] %}
    {% set uses_blob_base_fee = row['TX_HAS_BLOB_BASE_FEE'] %}
    {% set uses_eip_1559 = row['TX_HAS_EIP_1559'] %}
{% endif %}
{# Prod DB Variables End #}

{% set uses_receipts_by_hash = var('GLOBAL_USES_RECEIPTS_BY_HASH', false) %}
{% set gold_full_refresh = var('GOLD_FULL_REFRESH', false) %}
{% set unique_key = "tx_hash" if uses_receipts_by_hash else "block_number" %}

{# Log configuration details #}
{{ log_model_details() }}

{% if not gold_full_refresh %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    full_refresh = gold_full_refresh,
    tags = ['gold_core']
) }}

{% else %}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = unique_key,
    cluster_by = ['block_timestamp::DATE'],
    incremental_predicates = [fsc_evm.standard_predicate()],
    tags = ['gold_core']
) }}

{% endif %}

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
            utils.udf_hex_to_int(
                transaction_json :gasPrice :: STRING
            ) :: bigint AS gas_price,
            transaction_json :hash :: STRING AS tx_hash,
            transaction_json :input :: STRING AS input_data,
            LEFT(
                input_data,
                10
            ) AS origin_function_signature,
            {% if uses_mint %}
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
            {% if uses_source_hash %}
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
            {% if uses_eip_1559 %}
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_fee_per_gas,
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    transaction_json :maxPriorityFeePerGas :: STRING
                )
                    ) / pow(
                        10,
                        9
            ) AS max_priority_fee_per_gas,
            {% endif %}
            {% if uses_eth_value %}
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
            {% if uses_y_parity %}
            utils.udf_hex_to_int(transaction_json :yParity :: STRING):: bigint AS y_parity,
            {% endif %}
            {% if uses_access_list %}
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
            {% if uses_mint %}
            txs.mint,
            txs.mint_precise_raw,
            txs.mint_precise,
            {% endif %}
            {% if uses_eth_value %}
            txs.eth_value,
            txs.eth_value_precise_raw,
            txs.eth_value_precise,
            {% endif %}
            txs.value,
            txs.value_precise_raw,
            txs.value_precise,
            {% if uses_eip_1559 %}
            txs.max_fee_per_gas,
            txs.max_priority_fee_per_gas,
            {% endif %}
            {% if uses_l1_columns %}
            utils.udf_hex_to_int(r.receipts_json :l1Fee :: STRING) as l1_fee_precise_raw,
            COALESCE(
                l1_fee_precise_raw :: FLOAT,
                0
            ) AS l1_fee,
            utils.udf_decimal_adjust(l1_fee_precise_raw, 18) as l1_fee_precise,
            COALESCE(
                (
                    r.receipts_json :l1FeeScalar :: STRING
                ) :: FLOAT,
                0
            ) AS l1_fee_scalar,
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1GasUsed :: STRING
                ) :: FLOAT,
                0
            ) AS l1_gas_used,
            COALESCE(
                utils.udf_hex_to_int(
                    r.receipts_json :l1GasPrice :: STRING
                ) :: FLOAT,
                0
            ) AS l1_gas_price,
            utils.udf_hex_to_int(r.receipts_json :l1BaseFeeScalar :: STRING):: bigint AS l1_base_fee_scalar,
            {% endif %}
            {% if uses_y_parity %}
            txs.y_parity,
            {% endif %}
            {% if uses_access_list %}
            txs.access_list,
            {% endif %}
            {% if uses_blob_base_fee %}
            utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFee :: STRING):: bigint AS l1_blob_base_fee,
            utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFeeScalar :: STRING):: bigint AS l1_blob_base_fee_scalar,
            {% endif %}
            {% if uses_l1_tx_fee_calc %}
            utils.udf_decimal_adjust(
                (
                    txs.gas_price * utils.udf_hex_to_int(
                        r.receipts_json :gasUsed :: STRING
                    ) :: bigint
                ) + FLOOR(
                    l1_gas_price * l1_gas_used * l1_fee_scalar
                ) + IFF(
                    l1_fee_scalar = 0,
                    l1_fee,
                    0
                ),
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
            txs.nonce,
            txs.tx_position,
            txs.input_data,
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
            txs.r,
            txs.s,
            {% if uses_source_hash %}
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
{% if uses_receipts_by_hash %}
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
        {% if uses_mint %}
        t.mint,
        t.mint_precise_raw,
        t.mint_precise,
        {% endif %}
        {% if uses_eth_value %}
        t.eth_value,
        t.eth_value_precise_raw,
        t.eth_value_precise,
        {% endif %}
        t.value,
        t.value_precise_raw,
        t.value_precise,
        {% if uses_eip_1559 %}
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        {% endif %}
        {% if uses_l1_columns %}
        utils.udf_hex_to_int(r.receipts_json :l1Fee :: STRING) as l1_fee_precise_raw_heal,
        COALESCE(
            l1_fee_precise_raw_heal :: FLOAT,
            0
        ) AS l1_fee_heal,
        utils.udf_decimal_adjust(l1_fee_precise_raw_heal, 18) as l1_fee_precise_heal,
        COALESCE(
            (
                r.receipts_json :l1FeeScalar :: STRING
            ) :: FLOAT,
            0
        ) AS l1_fee_scalar_heal,
        COALESCE(
            utils.udf_hex_to_int(
                r.receipts_json :l1GasUsed :: STRING
            ) :: FLOAT,
            0
        ) AS l1_gas_used_heal,
        COALESCE(
            utils.udf_hex_to_int(
                r.receipts_json :l1GasPrice :: STRING
            ) :: FLOAT,
            0
        ) AS l1_gas_price_heal,
        utils.udf_hex_to_int(r.receipts_json :l1BaseFeeScalar :: STRING):: bigint AS l1_base_fee_scalar,
        {% endif %}
        {% if uses_y_parity %}
        t.y_parity,
        {% endif %}
        {% if uses_access_list %}
        t.access_list,
        {% endif %}
        {% if uses_blob_base_fee %}
        utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFee :: STRING):: bigint AS l1_blob_base_fee,
        utils.udf_hex_to_int(r.receipts_json :l1BlobBaseFeeScalar :: STRING):: bigint AS l1_blob_base_fee_scalar,
        {% endif %}
        {% if uses_l1_tx_fee_calc %}
        utils.udf_decimal_adjust(
            (
                t.gas_price * utils.udf_hex_to_int(
                    r.receipts_json :gasUsed :: STRING
                ) :: bigint
            ) + FLOOR(
                l1_gas_price * l1_gas_used * l1_fee_scalar
            ) + IFF(
                l1_fee_scalar = 0,
                l1_fee,
                0
            ),
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
        t.nonce,
        t.tx_position,
        t.input_data,
        t.gas_price,
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
        t.r,
        t.s,
        {% if uses_source_hash %}
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
        {% if uses_receipts_by_hash %}
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
        {% if uses_mint %}
        mint,
        mint_precise_raw,
        mint_precise,
        {% endif %}
        {% if uses_eth_value %}
        eth_value,
        eth_value_precise_raw,
        eth_value_precise,
        {% endif %}
        VALUE,
        value_precise_raw,
        value_precise,
        {% if uses_eip_1559 %}
        max_fee_per_gas,
        max_priority_fee_per_gas,
        {% endif %}
        {% if uses_l1_columns %}
        l1_fee,
        l1_fee_precise_raw,
        l1_fee_precise,
        l1_fee_scalar,
        l1_gas_used,
        l1_gas_price,
        l1_base_fee_scalar,
        {% endif %}
        {% if uses_y_parity %}
        y_parity,
        {% endif %}
        {% if uses_access_list %}
        access_list,
        {% endif %}
        {% if uses_blob_base_fee %}
        l1_blob_base_fee,
        l1_blob_base_fee_scalar,
        {% endif %}
        tx_fee,
        tx_fee_precise,
        tx_succeeded,
        tx_type,
        nonce,
        tx_position,
        input_data,
        gas_price,
        gas_used,
        gas_limit,
        cumulative_gas_used,
        effective_gas_price,
        r,
        s,
        {% if uses_source_hash %}
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
    {% if uses_mint %}
    mint,
    mint_precise_raw,
    mint_precise,
    {% endif %}
    {% if uses_eth_value %}
    eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    {% endif %}
    VALUE,
    value_precise_raw,
    value_precise,
    {% if uses_eip_1559 %}
    max_fee_per_gas,
    max_priority_fee_per_gas,
    {% endif %}
    {% if uses_l1_columns %}
    l1_fee_precise_heal AS l1_fee,
    l1_fee_precise_raw_heal AS l1_fee_precise_raw,
    l1_fee_precise_heal AS l1_fee_precise,
    l1_fee_scalar_heal AS l1_fee_scalar,
    l1_gas_used_heal AS l1_gas_used,
    l1_gas_price_heal AS l1_gas_price,
    l1_base_fee_scalar,
    {% endif %}
    {% if uses_y_parity %}
    y_parity,
    {% endif %}
    {% if uses_access_list %}
    access_list,
    {% endif %}
    {% if uses_blob_base_fee %}
    l1_blob_base_fee,
    l1_blob_base_fee_scalar,
    {% endif %}
    tx_fee_heal AS tx_fee,
    tx_fee_precise_heal AS tx_fee_precise,
    tx_succeeded_heal AS tx_succeeded,
    tx_type,
    nonce,
    tx_position,
    input_data,
    gas_price,
    gas_used_heal AS gas_used,
    gas_limit,
    cumulative_gas_used_heal AS cumulative_gas_used,
    effective_gas_price_heal AS effective_gas_price,
    r,
    s,
    {% if uses_source_hash %}
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
    tx_fee_precise,
    tx_succeeded,
    tx_type,
    nonce,
    tx_position,
    input_data,
    gas_price,
    gas_used,
    gas_limit,
    cumulative_gas_used,
    effective_gas_price,
    {% if uses_eip_1559 %}
    max_fee_per_gas,
    max_priority_fee_per_gas,
    {% endif %}
    {% if uses_l1_columns %}
    l1_fee,
    l1_fee_precise_raw,
    l1_fee_precise,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    l1_base_fee_scalar,
    {% endif %}
    {% if uses_blob_base_fee %}
    l1_blob_base_fee,
    l1_blob_base_fee_scalar,
    {% endif %}
    {% if uses_mint %}
    mint,
    mint_precise_raw,
    mint_precise,
    {% endif %}
    {% if uses_eth_value %}
    eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    {% endif %}
    {% if uses_y_parity %}
    y_parity,
    {% endif %}
    {% if uses_access_list %}
    access_list,
    {% endif %}
    r,
    s,
    v,
    {% if uses_source_hash %}
    source_hash,
    {% endif %}
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_transactions qualify ROW_NUMBER() over (
        PARTITION BY fact_transactions_id
        ORDER BY
            block_number DESC,
            block_timestamp DESC nulls last,
            tx_succeeded DESC nulls last
    ) = 1