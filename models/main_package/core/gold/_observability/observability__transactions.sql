{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false,
    tags = ['observability']
) }}

{% if is_incremental() %}
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
                ) {% if var('OBSERV_FULL_TEST') %}
                UNION ALL
                SELECT
                    0
                {% endif %}
        )
),
base AS (
    {% else %}
        WITH base AS (
    {% endif %}
    SELECT
        block_number, block_timestamp, tx_hash
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, systimestamp())

{% if is_incremental() %}
AND block_number >= (
SELECT
    block_number
FROM
    lookback)
{% endif %}),
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
        {{ ref("silver__confirm_blocks") }}
        b
        LEFT JOIN base t USING (
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
        missing_block_number IS NOT NULL
        AND missing_block_number NOT IN (
            SELECT
                block_number
            FROM
                {{ ref('silver_observability__exclusion_list') }}
        )
)
SELECT
    'transactions' AS test_name,
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
