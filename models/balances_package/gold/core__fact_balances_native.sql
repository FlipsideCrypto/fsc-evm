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
        post_state_json
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
            f.key AS address,
            f.value :nonce :: bigint AS nonce,
            f.value :balance :: STRING AS hex_balance
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => pre_state_json
            ) f
        WHERE
            f.value :balance IS NOT NULL
    ),
    post_state AS (
        SELECT
            block_number,
            tx_position,
            tx_hash,
            post_state_json,
            f.key AS address,
            f.value :nonce :: bigint AS nonce,
            f.value :balance :: STRING AS hex_balance
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => post_state_json
            ) f
        WHERE
            f.value :balance IS NOT NULL
    ),
    balances AS (
        SELECT
            p.block_number,
            b.block_timestamp,
            p.tx_position,
            p.tx_hash,
            p.address,
            p.nonce AS pre_nonce,
            p.hex_balance AS pre_hex_balance,
            utils.udf_hex_to_int(
                p.hex_balance
            ) :: bigint AS pre_raw_balance,
            utils.udf_decimal_adjust(
                pre_raw_balance,
                18
            ) AS pre_state_balance,
            COALESCE(
                pt.nonce,
                p.nonce
            ) AS post_nonce,
            COALESCE(
                pt.hex_balance,
                p.hex_balance
            ) AS post_hex_balance,
            utils.udf_hex_to_int(COALESCE(pt.hex_balance, p.hex_balance)) :: bigint AS post_raw_balance,
            utils.udf_decimal_adjust(
                post_raw_balance,
                18
            ) AS post_state_balance,
            post_raw_balance - pre_raw_balance AS net_raw_balance
        FROM
            pre_state p
            LEFT JOIN post_state pt USING(
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
        pre_state_balance,
        post_nonce,
        post_hex_balance,
        post_raw_balance,
        post_state_balance,
        net_raw_balance
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
        pre_state_balance,
        post_nonce,
        post_hex_balance,
        post_raw_balance,
        post_state_balance,
        net_raw_balance
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
    pre_state_balance,
    post_nonce,
    post_hex_balance,
    post_raw_balance,
    post_state_balance,
    net_raw_balance
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
    pre_state_balance,
    post_nonce,
    post_hex_balance,
    post_raw_balance,
    post_state_balance,
    net_raw_balance,
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
