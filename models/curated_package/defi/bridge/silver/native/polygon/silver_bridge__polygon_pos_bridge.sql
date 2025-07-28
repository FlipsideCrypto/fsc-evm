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

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    NULL AS event_index,
    to_address AS bridge_address,
    NULL AS event_name,
    from_address AS sender,
    from_address AS receiver,
    from_address AS destination_chain_receiver,
    '1' AS destination_chain_id,
    'ethereum' AS destination_chain,
    to_address AS token_address,
    NAME,
    utils.udf_hex_to_int(regexp_substr_all(SUBSTR(input, 11), '.{64}') [0] :: STRING) :: INT AS amount_unadj,
    'polygon_pos_bridge-v1' platform,
    'polygon_pos_bridge' AS protocol,
    'v1' AS version,
    'native' AS TYPE,
    fact_traces_id AS _id,
    inserted_timestamp,
    t.modified_timestamp
FROM
    {{ ref('core__fact_traces') }}
    t
    INNER JOIN {{ ref('silver_bridge__polygon_pos_contracts') }} C
    ON C.address = t.to_address
WHERE
    LEFT(
        input,
        10
    ) = '0x2e1a7d4d' -- withdraw
    AND block_timestamp :: DATE >= '2020-05-30'
    AND TYPE = 'CALL'
    AND '{{ vars.GLOBAL_PROJECT_NAME }}' = 'polygon'
    AND trace_succeeded
    AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
