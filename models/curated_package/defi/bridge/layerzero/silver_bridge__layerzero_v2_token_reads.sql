{# Set variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "contract_address",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH new_tokens AS (

    SELECT
        DISTINCT contract_address
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2024-01-01'
        AND topic_0 = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a' --OFTSent

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "12 hours") }}'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
),
ready_reads AS (
    SELECT
        contract_address,
        '0xfc0c546a' AS function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, 'latest']
        ) AS rpc_request
        FROM new_tokens
),
node_call AS (
    SELECT
        contract_address,
        live.udf_api(
            'POST',
            '{URL}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            rpc_request,
            '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
        ) AS response,
        '0x' || LOWER(SUBSTR(response :data :result :: STRING, 27, 40)) AS token_address
    FROM
        ready_reads
)
SELECT
    response,
    contract_address,
    IFF(
        token_address = '0x0000000000000000000000000000000000000000',
        '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
        token_address
    ) AS token_address,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
FROM
    node_call
