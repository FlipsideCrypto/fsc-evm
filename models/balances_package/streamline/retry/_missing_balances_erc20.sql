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
            PARTITION BY address,
            contract_address
            ORDER BY
                block_number,
                tx_position ASC
        ) AS prev_block_number,
        tx_position,
        address,
        contract_address,
        pre_balance_raw,
        pre_balance_precise,
        LAG(
            post_balance_precise,
            1
        ) over (
            PARTITION BY address,
            contract_address
            ORDER BY
                block_number,
                tx_position ASC
        ) AS prev_post_balance_precise
    FROM
        {{ ref("test_gold__fact_balances_erc20_recent") }}
    WHERE
        block_timestamp > DATEADD('day', -5, SYSDATE())),
        diffs AS (
            SELECT
                block_number,
                prev_block_number,
                tx_position,
                address,
                contract_address,
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
        missing_transfers AS (
            SELECT
                l.block_number,
                l.block_timestamp,
                l.tx_position,
                l.tx_hash,
                l.event_index,
                l.contract_address,
                CONCAT('0x', SUBSTR(l.topic_1, 27, 40)) :: STRING AS from_address,
                CONCAT('0x', SUBSTR(l.topic_2, 27, 40)) :: STRING AS to_address,
                utils.udf_hex_to_int(SUBSTR(l.data, 3, 64)) AS raw_amount_precise,
                TRY_TO_NUMBER(raw_amount_precise) AS raw_amount,
                l.tx_succeeded,
                d.address AS diff_address,
                d.prev_block_number AS diff_prev_block,
                d.block_number AS diff_block,
                d.pre_balance_raw AS expected_balance_change
            FROM
                {{ ref("test_gold__fact_event_logs_recent") }}
                l
                INNER JOIN diffs d
                ON l.contract_address = d.contract_address
                AND l.block_number > d.prev_block_number
                AND l.block_number < d.block_number
                AND (
                    from_address = d.address
                    OR to_address = d.address
                )
            WHERE
                l.block_timestamp > DATEADD('day', -5, SYSDATE())
                AND l.topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                AND l.topic_1 IS NOT NULL
                AND l.topic_2 IS NOT NULL
                AND l.data IS NOT NULL
                AND raw_amount IS NOT NULL
            )
        SELECT
            DISTINCT block_number
        FROM
            missing_transfers
        {% endif %}
