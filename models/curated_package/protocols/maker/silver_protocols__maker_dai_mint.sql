{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_timestamp', 'tx_hash', 'usr'],
    cluster_by = ['block_timestamp'],
    tags = ['silver_protocols', 'maker', 'dai_mint', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT
    block_timestamp,
    tx_hash,
    to_address AS usr,
    raw_amount_precise AS wad,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('core__ez_token_transfers') }}
WHERE from_address = '0x0000000000000000000000000000000000000000'
AND LOWER(contract_address) = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
{% if is_incremental() %}
AND block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
{% endif %}
