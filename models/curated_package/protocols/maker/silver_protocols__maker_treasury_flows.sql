{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code', 'token'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'treasury_flows', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH treasury_flows_preunioned AS (
    SELECT
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM {{ ref('core__ez_token_transfers') }} evt
    JOIN {{ ref('dim_treasury_erc20s') }} t
        ON evt.contract_address = t.contract_address
    WHERE evt.to_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    {% if is_incremental() %}
    AND evt.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token

    UNION ALL

    SELECT
        evt.block_timestamp AS ts,
        evt.tx_hash AS hash,
        t.token,
        -SUM(evt.RAW_AMOUNT_PRECISE / POW(10, t.decimals)) AS value
    FROM {{ ref('core__ez_token_transfers') }} evt
    JOIN {{ ref('dim_treasury_erc20s') }} t
        ON evt.contract_address = t.contract_address
    WHERE evt.from_address = '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    AND evt.to_address != '0xbe8e3e3618f7474f8cb1d074a26affef007e98fb'
    {% if is_incremental() %}
    AND evt.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
    GROUP BY evt.block_timestamp, evt.tx_hash, t.token
)

SELECT
    ts,
    hash,
    33110 AS code,
    value,
    token,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM treasury_flows_preunioned

UNION ALL

SELECT
    ts,
    hash,
    14620 AS code,
    value,
    token,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM treasury_flows_preunioned
