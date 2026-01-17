{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['ts', 'hash', 'code'],
    cluster_by = ['ts'],
    tags = ['silver_protocols', 'maker', 'opex', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH opex_preunion AS (
    SELECT
        mints.block_timestamp AS ts,
        mints.tx_hash AS hash,
        CASE
            WHEN dao_wallet.code IN ('GELATO', 'KEEP3R', 'CHAINLINK', 'TECHOPS') THEN 31710
            WHEN dao_wallet.code = 'GAS' THEN 31630
            WHEN dao_wallet.code IS NOT NULL THEN 31720
            ELSE 31740
        END AS equity_code,
        mints.wad / POW(10, 18) AS expense
    FROM {{ ref('silver_protocols__maker_dai_mint') }} mints
    JOIN {{ ref('silver_protocols__maker_opex_suck_hashes') }} opex
        ON mints.tx_hash = opex.tx_hash
    LEFT JOIN {{ ref('dim_dao_wallet') }} dao_wallet
        ON mints.usr = dao_wallet.wallet_address
    LEFT JOIN {{ ref('maker__fact_vat_frob') }} AS frobs
        ON mints.tx_hash = frobs.tx_hash
        AND mints.wad::NUMBER / 1e18 = frobs.dart
    WHERE frobs.tx_hash IS NULL
    {% if is_incremental() %}
    AND mints.block_timestamp >= DATEADD('hour', -{{ vars.CURATED_LOOKBACK_HOURS }}, SYSDATE())
    {% endif %}
)

SELECT
    ts,
    hash,
    equity_code AS code,
    -CAST(expense AS DOUBLE) AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM opex_preunion

UNION ALL

SELECT
    ts,
    hash,
    21120 AS code,
    expense AS value,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM opex_preunion
