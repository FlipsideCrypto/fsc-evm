{% set observ_uses_exclusion_list_blocks = var(
    'OBSERV_USES_EXCLUSION_LIST_BLOCKS',
    false
) %}

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

{% if var('OBSERV_FULL_TEST') %}
UNION ALL
SELECT
    0
{% endif %}
)
),
base AS (
    SELECT
        block_number,
        block_timestamp
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, systimestamp())

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
generated_range AS (
    SELECT
        _id AS block_number,
        min_block,
        max_block,
        min_block_timestamp,
        max_block_timestamp,
        blocks_tested
    FROM
        {{ ref('utils__number_sequence') }}
        INNER JOIN summary_stats
    WHERE
        _id BETWEEN min_block
        AND max_block
),
gap_test AS (
    SELECT
        IFF(
            b.block_number IS NULL,
            g.block_number,
            NULL
        ) AS missing_block_number
    FROM
        generated_range g
        LEFT JOIN base b USING (block_number)
),
gap_agg AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(missing_block_number) within GROUP (
            ORDER BY
                missing_block_number ASC
        ) AS missing_list
    FROM
        gap_test
    WHERE
        missing_block_number IS NOT NULL {% if observ_uses_exclusion_list_blocks %}
            AND missing_block_number NOT IN (
                SELECT
                    block_number
                FROM
                    {{ ref('silver_observability__exclusion_list') }}
            )
        {% endif %}
)
SELECT
    'blocks' AS test_name,
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
