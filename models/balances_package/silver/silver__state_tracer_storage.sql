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
            AND partition_key <= {{ vars.BALANCES_SILVER_STATE_TRACER_FR_MAX_BLOCK }}
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
    ),
state_tracer_final AS (
    SELECT
        pre.partition_key,
        pre.block_number,
        pre.tx_position,
        pre.tx_hash,
        pre.pre_state_json,
        post.post_state_json,
        pre.address,
        pre_nonce,
        pre_hex_balance,
        pre_storage,
        post_nonce,
        post_hex_balance,
        post_storage,
        pre._inserted_timestamp
    FROM
        pre_state pre
        LEFT JOIN post_state post USING(
            block_number,
            tx_position,
            address
        )
),
pre_state_storage AS (
    SELECT
        partition_key,
        block_number,
        tx_position,
        tx_hash,
        pre_state_json,
        address,
        pre_storage,
        pre.key :: STRING AS storage_key,
        pre.value :: STRING AS pre_storage_value_hex,
        _inserted_timestamp
    FROM
        state_tracer_final,
        LATERAL FLATTEN(
            input => pre_storage
        ) pre
),
post_state_storage AS (
    SELECT
        partition_key,
        block_number,
        tx_position,
        tx_hash,
        post_state_json,
        address,
        post_storage,
        post.key :: STRING AS storage_key,
        post.value :: STRING AS post_storage_value_hex,
        _inserted_timestamp
    FROM
        state_tracer_final,
        LATERAL FLATTEN(
            input => post_storage
        ) post
),
state_storage AS (
    SELECT
        partition_key,
        block_number,
        COALESCE(
            pre.tx_position,
            post.tx_position
        ) AS tx_position,
        COALESCE(
            pre.tx_hash,
            post.tx_hash
        ) AS tx_hash,
        COALESCE(
            pre.address,
            post.address
        ) AS contract_address,
        COALESCE(
            pre.storage_key,
            post.storage_key
        ) AS storage_key,
        COALESCE(
            pre_storage_value_hex,
            '0x0000000000000000000000000000000000000000000000000000000000000000'
        ) AS pre_storage_hex,
        COALESCE(
            post_storage_value_hex,
            '0x0000000000000000000000000000000000000000000000000000000000000000'
        ) AS post_storage_hex,
        _inserted_timestamp
    FROM
        pre_state_storage pre full
        OUTER JOIN post_state_storage post USING (
            block_number,
            tx_position,
            address,
            storage_key
        )
)
SELECT 
    partition_key,
    block_number,
    tx_position,
    tx_hash,
    contract_address,
    storage_key,
    pre_storage_hex,
    post_storage_hex,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position', 'contract_address', 'storage_key']) }} AS state_tracer_storage_id,
    _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    state_storage