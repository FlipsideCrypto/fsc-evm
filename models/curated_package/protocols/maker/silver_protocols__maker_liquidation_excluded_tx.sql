{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash'],
    cluster_by = ['tx_hash'],
    tags = ['silver_protocols', 'maker', 'liquidation', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT
    t.tx_hash,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__fact_traces') }} t
JOIN {{ ref('dim_maker_contracts') }} c
    ON t.from_address = c.contract_address
    AND c.contract_type IN ('FlapFlop')
{% if is_incremental() %}
WHERE t.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}

UNION

SELECT
    '0x395e70dfbb3b3a23fbfd0e7a4ad659c77302e2f5923606e006e981097cc27ef9' AS tx_hash,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
