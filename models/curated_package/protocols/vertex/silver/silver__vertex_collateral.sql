{# Set variables #}
{%- set clearinghouse = get_var('CURATED_VERTEX_CLEARINGHOUSE_CONTRACT', '') -%}
{%- set token_mapping = get_var('TOKEN_MAPPING',{}) -%}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_event_logs_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = get_path_tags(model),
    enabled = false
) }}


WITH logs_pull AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'ModifyCollateral' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LEFT(
            topics [1] :: STRING,
            42
        ) AS trader,
        topics [1] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS product_id,
        fact_event_logs_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xfe53084a731040f869d38b1dcd00fbbdbc14e10d7d739160559d77f5bc80cf05'
        AND contract_address = '{{ clearinghouse }}'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
product_id_join AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        CASE
            WHEN amount < 0 THEN 'withdraw'
            WHEN amount > 0 THEN 'deposit'
            WHEN amount = 0 THEN 'no-change'
        END AS modification_type,
        trader,
        subaccount,
        l.product_id,
        p.symbol,
        CASE
            {% for symbol, address in token_mapping.items() %}
                WHEN p.symbol = '{{ symbol }}' THEN '{{ address }}'
            {% endfor %}
        END AS token_address,
        amount,
        l.fact_event_logs_id,
        l.modified_timestamp
    FROM
        logs_pull l
        LEFT JOIN {{ ref('silver__vertex_dim_products') }}
        p
        ON l.product_id = p.product_id
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        A.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        modification_type,
        trader,
        subaccount,
        product_id,
        A.symbol,
        A.token_address,
        amount AS amount_unadj,
        amount / pow(10, 18) AS amount,
        (amount / pow(10, 18) * p.price) :: FLOAT AS amount_usd,
        A.fact_event_logs_id,
        A.modified_timestamp
    FROM
        product_id_join A
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON A.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.token_address = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS vertex_collateral_id,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify ROW_NUMBER() over(
        PARTITION BY fact_event_logs_id
        ORDER BY
            modified_timestamp DESC
    ) = 1
