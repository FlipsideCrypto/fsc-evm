{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'liquidation_revenue', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp AS ts,
    tx_hash AS hash,
    SUM(CAST(rad AS DOUBLE)) AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('maker__fact_vat_move') }}
WHERE
    dst_address = '0xa950524441892a31ebddf91d3ceefa04bf454466'
    AND src_address NOT IN (SELECT contract_address FROM {{ ref('dim_maker_contracts') }})
    AND src_address NOT IN (
        '0xa13c0c8eb109f5a13c6c90fc26afb23beb3fb04a',
        '0x621fe4fde2617ea8ffade08d0ff5a862ad287ec2'
    )
    AND tx_hash NOT IN (SELECT tx_hash FROM {{ ref('silver_protocols__maker_liquidation_excluded_tx') }})
    AND tx_hash NOT IN (SELECT tx_hash FROM {{ ref('fact_team_dai_burns_tx') }})
    AND tx_hash NOT IN (SELECT tx_hash FROM {{ ref('fact_psm_yield_tx') }})
    AND tx_hash NOT IN (SELECT tx_hash FROM {{ ref('fact_rwa_yield_tx') }})
{% if is_incremental() %}
    AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
GROUP BY block_timestamp, tx_hash
