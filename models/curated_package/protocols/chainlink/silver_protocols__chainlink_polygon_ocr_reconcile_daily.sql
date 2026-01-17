{{ config(
    materialized = 'table',
    tags = ['silver_protocols', 'chainlink', 'ocr', 'reconcile', 'polygon', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{#
    Chainlink Polygon OCR Reconciliation Daily

    This model reconciles OCR rewards data from October 2023.
    Contains historical reconciliation adjustments for operator payments.
#}

WITH reconcile_20231017_polygon_evt_transfer AS (
    SELECT
        evt_transfer.from_address AS admin_address
        , MAX(amount) AS token_value
    FROM {{ ref('core__ez_token_transfers') }} evt_transfer
    LEFT JOIN {{ ref('dim_chainlink_polygon_ocr_operator_admin_meta') }} ocr_operator_admin_meta
        ON ocr_operator_admin_meta.admin_address = evt_transfer.from_address
    WHERE evt_transfer.block_timestamp >= '2023-10-16'
        AND evt_transfer.to_address = LOWER('0x2431d49d225C1BcCE7541deA6Da7aEf9C7AD3e23')
    GROUP BY
        evt_transfer.tx_hash
        , evt_transfer.event_index
        , evt_transfer.from_address
)
, reconcile_20231017_polygon_daily AS (
    SELECT
        '2023-10-16' AS date_start
        , CAST(DATE_TRUNC('month', CAST('2023-10-16' AS DATE)) AS DATE) AS date_month
        , admin_address
        , 0 - SUM(token_value) AS token_amount
    FROM reconcile_20231017_polygon_evt_transfer
    GROUP BY 3
)
, reconcile_20231017_ethereum_evt_transfer AS (
    SELECT
        evt_transfer.from_address AS admin_address
        , MAX(amount) AS token_value
    FROM {{ ref('core__ez_token_transfers') }} evt_transfer
    LEFT JOIN {{ ref('dim_chainlink_polygon_ocr_operator_admin_meta') }} ocr_operator_admin_meta
        ON ocr_operator_admin_meta.admin_address = evt_transfer.from_address
    WHERE evt_transfer.block_timestamp >= '2023-10-16'
        AND evt_transfer.from_address = LOWER('0xC489244f2a5FC0E65A0677560EAA4A13F5036ab6')
    GROUP BY
        evt_transfer.tx_hash
        , evt_transfer.event_index
        , evt_transfer.from_address
)
, reconcile_20231017_ethereum_daily AS (
    SELECT
        '2023-10-16' AS date_start
        , CAST(DATE_TRUNC('month', CAST('2023-10-16' AS DATE)) AS DATE) AS date_month
        , admin_address
        , 0 - SUM(token_value) AS token_amount
    FROM reconcile_20231017_ethereum_evt_transfer
    GROUP BY 3
)

SELECT
    COALESCE(reconcile_polygon.date_start, reconcile_ethereum.date_start) AS date_start
    , COALESCE(reconcile_polygon.admin_address, reconcile_ethereum.admin_address) AS admin_address
    , COALESCE(reconcile_polygon.token_amount, 0) + COALESCE(reconcile_ethereum.token_amount, 0) AS token_amount
    , SYSDATE() AS inserted_timestamp
    , SYSDATE() AS modified_timestamp
    , '{{ invocation_id }}' AS _invocation_id
FROM reconcile_20231017_polygon_daily reconcile_polygon
FULL OUTER JOIN reconcile_20231017_ethereum_daily reconcile_ethereum
    ON reconcile_ethereum.admin_address = reconcile_polygon.admin_address
