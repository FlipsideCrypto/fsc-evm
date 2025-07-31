{% test balances_diffs_native(
    model,
    test_model
) %}

{# Get variables #}
{% set vars = return_vars() %}

WITH source AS (
    SELECT
        block_number,
        LAG(block_number, 1) over (
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
        {{ model }}
)
SELECT
    s.block_number,
    prev_block_number,
    t.block_number AS missing_block_number,
    s.tx_position,
    s.address,
    pre_balance_raw,
    pre_balance_precise,
    prev_post_balance_precise,
    pre_balance_precise - prev_post_balance_precise AS diff
FROM
    source s
    LEFT JOIN {{ ref(test_model) }} t 
    ON t.block_number > s.prev_block_number AND t.block_number < s.block_number
    AND (
        from_address = s.address
        OR to_address = s.address
        OR origin_from_address = s.address
        OR origin_to_address = s.address
    )
WHERE
    diff <> 0
    AND diff IS NOT NULL 
    AND t.block_number IS NOT NULL
    {% if vars.BALANCES_EXCLUSION_LIST_ENABLED %}
        AND address NOT IN (
            SELECT
                DISTINCT address
            FROM
                silver.validator_addresses
        )
    {% endif %}

    {% endtest %}

    {% test balances_diffs_erc20(
        model,
        test_model
    ) %}
    WITH source AS (
        SELECT
            block_number,
            LAG(block_number, 1) over (
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
            {{ model }}
    )
SELECT
    s.block_number,
    prev_block_number,
    t.block_number AS missing_block_number,
    s.tx_position,
    address,
    s.contract_address,
    pre_balance_raw,
    pre_balance_precise,
    prev_post_balance_precise,
    pre_balance_precise - prev_post_balance_precise AS diff
FROM
    source s 
    LEFT JOIN {{ ref(test_model) }} t 
    ON t.block_number > s.prev_block_number AND t.block_number < s.block_number
    AND (
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING = s.address
        OR CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING = s.address
    )
WHERE
    diff <> 0
    AND diff IS NOT NULL 
    AND t.block_number IS NOT NULL
    {% endtest %}
