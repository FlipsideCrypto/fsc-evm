{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        contract_address,
        '0x' || SUBSTR(
            topic_1,
            27
        ) :: STRING AS l1_token,
        '0x' || SUBSTR(
            topic_2,
            27
        ) :: STRING AS l2_token,
        '0x' || SUBSTR(
            topic_3,
            27
        ) :: STRING AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS part,
        '0x' || SUBSTR(
            part [0] :: STRING,
            25
        ) AS to_address,
        utils.udf_hex_to_int(
            part [1] :: STRING
        ) :: INT AS amount_unadj,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x4200000000000000000000000000000000000010' -- superchain l2 bridge
        AND topic_0 = '0x73d170910aba9e6d50b102db522b1dbcd796216f5128b445aa2135272886497e' -- withdrawal initiated
        AND block_timestamp :: DATE >= '2021-11-01'
        AND '{{ vars.GLOBAL_PROJECT_NAME }}' IN (
            'base',
            'bob',
            'ink',
            'optimism'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    contract_address AS bridge_address,
    'WithdrawalInitiated' AS event_name,
    from_address AS sender,
    to_address AS receiver,
    to_address AS destination_chain_receiver,
    '1' AS destination_chain_id,
    'ethereum' AS destination_chain,
    IFF(
        l1_token = '0x0000000000000000000000000000000000000000',
        '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
        l2_token
    ) AS token_address,
    amount_unadj,
    'superchain_l2_standard_bridge-v1' AS platform,
    'superchain_l2_standard_bridge' AS protocol,
    'v1' AS version,
    'native' AS TYPE,
    CONCAT(
        tx_hash,
        '-',
        event_index
    ) AS _id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
