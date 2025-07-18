{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'pancakeswap'
        AND version = 'v2'
        AND type = 'mm_router'
),
swaps AS (

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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS user_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS mm_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS nonce,
        CONCAT('0x', SUBSTR(l_segmented_data [1] :: STRING, 25, 40)) AS mmTreasury,
        CONCAT('0x', SUBSTR(l_segmented_data [2] :: STRING, 25, 40)) AS baseToken1,
        CONCAT('0x', SUBSTR(l_segmented_data [3] :: STRING, 25, 40)) AS quoteToken1,
        CASE
            WHEN baseToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            ELSE baseToken1
        END AS baseToken,
        CASE
            WHEN quoteToken1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            ELSE quoteToken1
        END AS quoteToken,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS baseTokenAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [5] :: STRING
            )
        ) AS quoteTokenAmount,
        baseToken AS token_in,
        quoteToken AS token_out,
        baseTokenAmount AS token_in_amount,
        quoteTokenAmount AS token_out_amount,
        m.protocol,
        m.version,
        m.type,
        CONCAT(
            m.protocol,
            '-',
            m.version
        ) AS platform,
        'Swap' AS event_name,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        topics [0] :: STRING = '0xe7d6f812e1a54298ddef0b881cd08a4d452d9de35eb18b5145aa580fdda18b26' --swap
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
    user_address AS sender,
    user_address AS tx_to,
    mm_address,
    nonce,
    mmTreasury,
    baseToken,
    quoteToken,
    baseTokenAmount,
    quoteTokenAmount,
    token_in,
    token_out,
    token_in_amount AS amount_in_unadj,
    token_out_amount AS amount_out_unadj,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    swaps
