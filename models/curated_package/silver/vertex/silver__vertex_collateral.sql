{# Set variables #}
{%- set clearinghouse = var('CLEARINGHOUSE_CONTRACT', '') -%}

{# Log configuration details #}
{%- if flags.WHICH == 'compile' and execute -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '",\n' %}
    {% set config_log = config_log ~ '    unique_key = "' ~ config.get('unique_key') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg'],
    enabled = false
) }}

{# Main query starts here #}
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
        _log_id,
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
            WHEN p.symbol = 'USDC' THEN '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'
            WHEN p.symbol = 'BENJI' THEN '0xbc45647ea894030a4e9801ec03479739fa2485f0'
            WHEN p.symbol = 'WETH' THEN '0x4300000000000000000000000000000000000006'
            WHEN p.symbol = 'ETH' THEN '0x4300000000000000000000000000000000000006'
            WHEN p.symbol = 'TRUMPWIN' THEN '0xe215d028551d1721c6b61675aec501b1224bd0a1'
            WHEN p.symbol = 'HARRISWIN' THEN '0xfbac82a384178ca5dd6df72965d0e65b1b8a028f'
        END AS token_address,
        amount,
        l._log_id,
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
        A._log_id,
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
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify ROW_NUMBER() over(
        PARTITION BY _log_id
        ORDER BY
            modified_timestamp DESC
    ) = 1
