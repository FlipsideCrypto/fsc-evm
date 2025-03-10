{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

WITH lookback AS (

    SELECT
        MIN(block_number) AS block_number
    FROM
        (
            SELECT
                block_number
            FROM
                {{ ref('core__fact_blocks') }}
            WHERE
                block_timestamp >= DATEADD('hour', -96, systimestamp())

{% if is_incremental() %}
UNION ALL
    (
        SELECT
            missing_list [0]
        FROM
            {{ this }}
        ORDER BY
            test_timestamp DESC
        LIMIT
            1
    )
{% endif %}

{% if vars.MAIN_OBSERV_FULL_TEST_ENABLED %}
UNION ALL
SELECT
    0
{% endif %}
)
),
base AS (
    SELECT
        block_number,
        b.block_timestamp,
        r.receipts_json :transactionHash :: STRING AS tx_hash
    FROM
        {{ ref('silver__receipts') }}
        r
        INNER JOIN {{ ref('core__fact_blocks') }}
        b USING (block_number)
    WHERE
        b.block_timestamp <= DATEADD('hour', -12, systimestamp())
        AND receipts_json :logs [0] IS NOT NULL

{% if is_incremental() %}
AND block_number >= (
    SELECT
        block_number
    FROM
        lookback
)
{% endif %}
),
summary_stats AS (
    SELECT
        MIN(block_number) AS min_block,
        MAX(block_number) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        base
),
gap_test AS (
    SELECT
        IFF(
            t.tx_hash IS NULL,
            b.block_number,
            NULL
        ) AS missing_block_number
    FROM
        base b
        LEFT JOIN {{ ref('core__fact_event_logs') }}
        t USING (
            block_number,
            tx_hash
        )
        INNER JOIN summary_stats
    WHERE
        t.tx_hash IS NULL
        AND b.block_number >= min_block
        AND b.block_number <= max_block
),
gap_agg AS (
    SELECT
        COUNT(
            DISTINCT missing_block_number
        ) AS blocks_impacted_count,
        ARRAY_AGG(
            DISTINCT missing_block_number
        ) within GROUP (
            ORDER BY
                missing_block_number ASC
        ) AS missing_list
    FROM
        gap_test
    WHERE
        missing_block_number IS NOT NULL {% if vars.MAIN_OBSERV_LOGS_EXCLUSION_LIST_ENABLED %}
            AND missing_block_number NOT IN (
                SELECT
                    block_number
                FROM
                    {{ ref('silver_observability__exclusion_list') }}
            )
        {% endif %}
)
SELECT
    'logs' AS test_name,
    systimestamp() AS test_timestamp,
    min_block,
    max_block,
    min_block_timestamp,
    max_block_timestamp,
    blocks_tested,
    blocks_impacted_count,
    missing_list
FROM
    gap_agg,
    summary_stats
