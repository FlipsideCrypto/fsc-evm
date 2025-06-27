{% test balances_diffs_native(
    model
) %}
WITH source AS (
    SELECT
        block_number,
        tx_position,
        address,
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
    block_number,
    tx_position,
    address,
    pre_balance_precise,
    prev_post_balance_precise,
    pre_balance_precise - prev_post_balance_precise AS diff
FROM
    source
WHERE
    diff <> 0
    AND diff IS NOT NULL 
{% endtest %}

{% test balances_diffs_erc20(
    model
) %}
    WITH source AS (
        SELECT
            block_number,
            tx_position,
            address,
            contract_address,
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
    block_number,
    tx_position,
    address,
    contract_address,
    pre_balance_precise,
    prev_post_balance_precise,
    pre_balance_precise - prev_post_balance_precise AS diff
FROM
    source
WHERE
    diff <> 0
    AND diff IS NOT NULL 
{% endtest %}
