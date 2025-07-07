{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH router_swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS account_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS tokenIn,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [6] :: STRING
            )
        ) AS amountOut,
        p.platform,
        p.protocol,
        p.version,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__hashflow_pools') }}
        p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0xb709ddcc6550418e9b89df1f4938071eeaa3f6376309904c77e15d46b16066f5' --Swap
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS account_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS tokenIn,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS amountOut,
        p.platform,
        p.protocol,
        p.version,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver_dex__hashflow_pools') }}
        p
        ON l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e' --Swap
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        event_name,
        contract_address,
        origin_from_address AS sender,
        account_address AS tx_to,
        tokenIn AS token_in,
        tokenOut AS token_out,
        amountIn AS amount_in_unadj,
        amountOut AS amount_out_unadj,
        platform,
        protocol,
        version,
        _log_id,
        modified_timestamp
    FROM
        router_swaps_base
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        event_name,
        contract_address,
        origin_from_address AS sender,
        account_address AS tx_to,
        tokenIn AS token_in,
        tokenOut AS token_out,
        amountIn AS amount_in_unadj,
        amountOut AS amount_out_unadj,
        platform,
        protocol,
        version,
        _log_id,
        modified_timestamp
    FROM
        swaps_base
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    event_name,
    contract_address,
    sender,
    tx_to,
    CASE
        WHEN token_in = '0x0000000000000000000000000000000000000000' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE token_in
    END AS token_in,
    CASE
        WHEN token_out = '0x0000000000000000000000000000000000000000' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE token_out
    END AS token_out,
    amount_in_unadj,
    amount_out_unadj,
    platform,
    protocol,
    version,
    _log_id,
    modified_timestamp
FROM
    FINAL
