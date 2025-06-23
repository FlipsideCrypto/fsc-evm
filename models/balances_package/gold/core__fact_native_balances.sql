{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['modified_timestamp::date', 'round(block_number, -3)'],
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
        _inserted_timestamp
    FROM
        {{ ref('silver__state_tracer') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
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
            f.value :balance :: STRING AS hex_balance,
            _inserted_timestamp
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
            f.value :balance :: STRING AS hex_balance,
            _inserted_timestamp
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => post_state_json
            ) f
        WHERE
            f.value :balance IS NOT NULL
    )
SELECT
    p.block_number,
    p.tx_position,
    p.tx_hash,
    p.address,
    p.nonce AS pre_nonce,
    p.hex_balance AS pre_hex_balance,
    utils.udf_hex_to_int(p.hex_balance) :: bigint AS pre_raw_balance,
    utils.udf_decimal_adjust(
        pre_raw_balance,
        18
    ) AS pre_state_balance,
    -- Post balance columns with fallback to pre balance
    COALESCE(pt.nonce, p.nonce) AS post_nonce,
    COALESCE(pt.hex_balance, p.hex_balance) AS post_hex_balance,
    utils.udf_hex_to_int(COALESCE(pt.hex_balance, p.hex_balance)) :: bigint AS post_raw_balance,
    utils.udf_decimal_adjust(
        post_raw_balance,
        18
    ) AS post_state_balance,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'address']) }} AS fact_native_balances_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    pre_state p
    LEFT JOIN post_state pt
    USING(block_number, tx_position, address)
