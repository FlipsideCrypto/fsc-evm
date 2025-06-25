{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'round(block_number, -3)'],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','phase_4']
) }}

WITH state_tracer AS (

    SELECT
        block_number,
        tx_position,
        tx_hash,
        pre_state_json,
        post_state_json,
        address,
        pre_nonce,
        pre_hex_balance,
        post_nonce,
        post_hex_balance
    FROM
        {{ ref('silver__state_tracer') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP)
        FROM
            {{ this }})
        {% endif %}
    ),
    pre_state AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            pre_state_json,
            address,
            pre_nonce AS nonce,
            pre_hex_balance AS hex_balance
        FROM
            state_tracer
        WHERE
            pre_hex_balance IS NOT NULL
    ),
    post_state AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            post_state_json,
            address,
            post_nonce AS nonce,
            post_hex_balance AS hex_balance
        FROM
            state_tracer
        WHERE
            post_hex_balance IS NOT NULL
    ),
    balances AS (
        SELECT
            pre.block_number,
            b.block_timestamp,
            pre.tx_position,
            pre.tx_hash,
            pre.address,
            pre.nonce AS pre_nonce,
            pre.hex_balance AS pre_hex_balance,
            utils.udf_hex_to_int(
                pre.hex_balance
            ) :: bigint AS pre_raw_balance,
            utils.udf_decimal_adjust(
                pre_raw_balance,
                18
            ) AS pre_balance_precise,
            pre_balance_precise :: FLOAT AS pre_balance,
            COALESCE(
                post.nonce,
                pre.nonce
            ) AS post_nonce,
            COALESCE(
                post.hex_balance,
                pre.hex_balance
            ) AS post_hex_balance,
            utils.udf_hex_to_int(COALESCE(post.hex_balance, pre.hex_balance)) :: bigint AS post_raw_balance,
            utils.udf_decimal_adjust(
                post_raw_balance,
                18
            ) AS post_balance_precise,
            post_balance_precise :: FLOAT AS post_balance,
            post_raw_balance - pre_raw_balance AS net_raw_balance,
            post_balance_precise - pre_balance_precise AS net_balance
        FROM
            pre_state pre
            LEFT JOIN post_state post USING(
                block_number,
                tx_position,
                address
            )
            LEFT JOIN {{ ref('core__fact_blocks') }}
            b USING(block_number)
    )

{% if is_incremental() %},
missing_data AS (
    SELECT
        block_number,
        b.block_timestamp AS block_timestamp_heal,
        tx_position,
        tx_hash,
        address,
        pre_nonce,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance_precise,
        pre_balance,
        post_nonce,
        post_hex_balance,
        post_raw_balance,
        post_balance_precise,
        post_balance,
        net_raw_balance,
        net_balance
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core__fact_blocks') }}
        b USING(block_number)
    WHERE
        t.block_timestamp IS NULL
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_position,
        tx_hash,
        address,
        pre_nonce,
        pre_hex_balance,
        pre_raw_balance,
        pre_balance_precise,
        pre_balance,
        post_nonce,
        post_hex_balance,
        post_raw_balance,
        post_balance_precise,
        post_balance,
        net_raw_balance,
        net_balance
    FROM
        balances

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
    tx_position,
    tx_hash,
    address,
    pre_nonce,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance_precise,
    pre_balance,
    post_nonce,
    post_hex_balance,
    post_raw_balance,
    post_balance_precise,
    post_balance,
    net_raw_balance,
    net_balance
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_position,
    tx_hash,
    address,
    pre_nonce,
    pre_hex_balance,
    pre_raw_balance,
    pre_balance_precise,
    pre_balance,
    post_nonce,
    post_hex_balance,
    post_raw_balance,
    post_balance_precise,
    post_balance,
    net_raw_balance,
    net_balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'address']) }} AS fact_balances_native_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL

{% if is_incremental() %}
qualify (ROW_NUMBER() over (PARTITION BY block_number, tx_position, address
ORDER BY
    modified_timestamp DESC)) = 1
{% endif %}
