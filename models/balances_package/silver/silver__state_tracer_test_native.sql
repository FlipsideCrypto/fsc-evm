{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__state_tracer') }}
-- depends_on: {{ ref('bronze__state_tracer_fr') }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['modified_timestamp::date', 'partition_key'],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','phase_4']
) }}

WITH state_tracer AS (

    SELECT
        partition_key,
        block_number,
        array_index AS tx_position,
        DATA AS state_json,
        DATA :txHash :: STRING AS tx_hash,
        DATA :result :pre :: variant AS pre_state_json,
        DATA :result :post :: variant AS post_state_json,
        _inserted_timestamp
    FROM

{% if is_incremental() and not vars.BALANCES_SILVER_STATE_TRACER_FULL_RELOAD_ENABLED %}
{{ ref('bronze__state_tracer') }}
WHERE
    _inserted_timestamp > (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
            AND DATA IS NOT NULL 
        {% elif is_incremental() and vars.BALANCES_SILVER_STATE_TRACER_FULL_RELOAD_ENABLED %}
            {{ ref('bronze__state_tracer_fr') }}
        WHERE
            DATA IS NOT NULL
            AND partition_key BETWEEN (
                SELECT
                    MAX(
                        partition_key
                    )
                FROM
                    {{ this }}
            )
            AND (
                SELECT
                    MAX(
                        partition_key
                    ) + {{ vars.BALANCES_SILVER_STATE_TRACER_FULL_RELOAD_BLOCKS_PER_RUN }}
                FROM
                    {{ this }}
            )
        {% else %}
            {{ ref('bronze__state_tracer_fr') }}
        WHERE
            DATA IS NOT NULL
            AND partition_key >= 25000000 --temp
            {# AND partition_key <= {{ vars.BALANCES_SILVER_STATE_TRACER_FR_MAX_BLOCK }} #}
        {% endif %}

        qualify (ROW_NUMBER() over (PARTITION BY block_number, tx_position
        ORDER BY
            _inserted_timestamp DESC)) = 1
    ),
    pre_state AS (
        SELECT
            partition_key,
            block_number,
            tx_position,
            tx_hash,
            pre_state_json,
            pre.key AS address,
            pre.value :nonce :: bigint AS pre_nonce,
            pre.value :balance :: STRING AS pre_hex_balance,
            pre.value :storage :: variant AS pre_storage,
            _inserted_timestamp
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => pre_state_json
            ) pre
    ),
    post_state AS (
        SELECT
            partition_key,
            block_number,
            tx_position,
            tx_hash,
            post_state_json,
            post.key AS address,
            post.value :nonce :: bigint AS post_nonce,
            post.value :balance :: STRING AS post_hex_balance,
            post.value :storage :: variant AS post_storage,
            _inserted_timestamp
        FROM
            state_tracer,
            LATERAL FLATTEN(
                input => post_state_json
            ) post
    )
SELECT
    pre.partition_key,
    pre.block_number,
    pre.tx_position,
    pre.tx_hash,
    pre.address,
    pre_nonce,
    pre_hex_balance,
    pre_storage,
    post_nonce,
    post_hex_balance,
    post_storage,
    {{ dbt_utils.generate_surrogate_key(['pre.block_number', 'pre.tx_position', 'pre.address']) }} AS state_tracer_id,
    _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_state pre
    LEFT JOIN post_state post USING(
        block_number,
        tx_position,
        address
    )
