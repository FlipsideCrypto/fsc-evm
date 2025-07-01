{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% if vars.BALANCES_SL_NEW_BUILD_ENABLED %}

    SELECT
        0 AS block_number
    {% else %}
    
    WITH source AS (
        SELECT
            block_number,
            LAG(
                block_number,
                1
            ) over (
                PARTITION BY address
                ORDER BY
                    block_number,
                    tx_position ASC
            ) AS prev_block_number,
            tx_position,
            address,
            pre_balance_raw,
            pre_balance_precise,
            LAG(
                post_balance_precise,
                1
            ) over (
                PARTITION BY address
                ORDER BY
                    block_number,
                    tx_position ASC
            ) AS prev_post_balance_precise
        FROM
            {{ ref("test_gold__fact_balances_native_recent") }}
        WHERE
            block_timestamp > DATEADD('day', -5, SYSDATE()) 
            {% if vars.BALANCES_EXCLUSION_LIST_ENABLED %}
                AND address NOT IN (
                    SELECT
                        DISTINCT address
                    FROM
                        silver.validator_addresses
                )
            {% endif %}
    ),
    diffs AS (
        SELECT
            block_number,
            prev_block_number,
            tx_position,
            address,
            pre_balance_raw,
            pre_balance_precise,
            prev_post_balance_precise,
            pre_balance_precise - prev_post_balance_precise AS diff
        FROM
            source
        WHERE
            diff <> 0
            AND diff IS NOT NULL
    ),
    missing_native_transfers AS (
        SELECT
            t.block_number,
            t.block_timestamp,
            t.tx_position,
            t.tx_hash,
            t.trace_index,
            from_address,
            to_address,
            origin_from_address,
            origin_to_address,
            value_precise_raw,
            VALUE,
            t.tx_succeeded,
            t.trace_succeeded,
            d.address AS diff_address,
            d.prev_block_number AS diff_prev_block,
            d.block_number AS diff_block,
            d.pre_balance_raw AS expected_balance_change
        FROM
            {{ ref("test_gold__fact_traces_recent") }}
            t
            INNER JOIN diffs d
            ON t.block_number > d.prev_block_number
            AND t.block_number < d.block_number
            AND (
                from_address = d.address
                OR to_address = d.address
                OR origin_from_address = d.address
                OR origin_to_address = d.address
            )
        WHERE
            t.block_timestamp > DATEADD('day', -5, SYSDATE())
    )
SELECT
    DISTINCT block_number
FROM
    missing_native_transfers
{% endif %}
