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

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'dodo'
        AND type = 'proxy'
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
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS fromToken,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS toToken,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS trader_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [5] :: STRING,
                25,
                40
            )
        ) AS receiver_address,
        p.platform,
        p.protocol,
        p.version,
        p.type,
        'dodoswap' AS event_name,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    INNER JOIN {{ ref('silver_dex__dodo_v2_pools') }} p
    ON
        l.contract_address = p.pool_address
    WHERE
        l.topics [0] :: STRING = '0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f' --dodoswap
        AND trader_address NOT IN (
            SELECT
                contract_address
            FROM
                contract_mapping
        )
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
    CASE
        WHEN fromToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE fromToken
    END AS token_in,
    CASE
        WHEN toToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
        ELSE toToken
    END AS token_out,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    trader_address AS sender,
    receiver_address AS tx_to,
    platform,
    protocol,
    version,
    type,
    _log_id,
    modified_timestamp
FROM
    swaps
