{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_event_logs_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','nado']
) }}


WITH health_groups AS (

    SELECT
        product_id,
        health_group,
        health_group_symbol
    FROM
        {{ ref('silver__nado_dim_products') }}
    GROUP BY
        ALL
),
logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        contract_address,
        data,
        event_index,
        event_removed,
        topics,
        fact_event_logs_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x494f937f5cc892f798248aa831acfb4ad7c4bf35edd8498c5fb431ce1e38b035'
        AND contract_address = '{{ vars.CURATED_NADO_CLEARINGHOUSE_CONTRACT }}'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
logs_pull_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Liquidation' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS product_id,
        topics [1] :: STRING AS digest,
        LEFT(
            topics [2] :: STRING,
            42
        ) AS trader,
        topics [2] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [3] :: STRING
        ) :: INT AS amount_quote,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS is_encoded_spread,
        fact_event_logs_id,
        modified_timestamp
    FROM
        logs
    WHERE
        topics [0] :: STRING = '0x494f937f5cc892f798248aa831acfb4ad7c4bf35edd8498c5fb431ce1e38b035'
        AND contract_address = '{{ vars.CURATED_NADO_CLEARINGHOUSE_CONTRACT }}'
),
v2_nado_decode AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        is_encoded_spread,
        digest,
        trader,
        subaccount,
        amount,
        amount_quote,
        CASE
            WHEN is_encoded_spread = 1 THEN utils.udf_int_to_binary(product_id)
            ELSE NULL
        END AS bin_product_ids,
        CASE
            WHEN is_encoded_spread = 1 THEN ARRAY_CONSTRUCT(
                utils.udf_binary_to_int(SUBSTR(bin_product_ids, -16)),
                utils.udf_binary_to_int(
                    SUBSTR(
                        bin_product_ids,
                        1,
                        GREATEST(len(bin_product_ids) - 16, 1))
                    )
                )
                ELSE NULL
            END AS decoded_spread_product_ids,
            CASE
                WHEN is_encoded_spread = 1 THEN decoded_spread_product_ids [0] :: STRING
                ELSE product_id
            END AS product_id,
            fact_event_logs_id,
            modified_timestamp
            FROM
                logs_pull_v2
        ),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        digest,
        trader,
        subaccount,
        l.product_id,
        p.health_group,
        p.health_group_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        amount_quote AS amount_quote_unadj,
        amount_quote / pow(
            10,
            18
        ) AS amount_quote,
        CASE
            WHEN is_encoded_spread = 1 THEN TRUE
            ELSE FALSE
        END AS is_encoded_spread,
        decoded_spread_product_ids AS spread_product_ids,
        l.fact_event_logs_id,
        l.modified_timestamp
    FROM
        v2_nado_decode l
        LEFT JOIN health_groups p
        ON l.product_id = p.product_id
)
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS nado_liquidation_id,
        SYSDATE() AS inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        FINAL qualify ROW_NUMBER() over(
            PARTITION BY fact_event_logs_id
            ORDER BY
                modified_timestamp DESC
        ) = 1
