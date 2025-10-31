{% test stablecoins_date_gaps(model, lookback_days=7) %}

WITH recent_data AS (
    SELECT
        contract_address,
        address,
        block_date
    FROM
        {{ model }}
    WHERE
        block_date >= DATEADD('day', -{{ lookback_days }}, SYSDATE())
),
source AS (
    SELECT
        contract_address,
        address,
        block_date,
        LAG(block_date, 1) OVER (
            PARTITION BY contract_address, address
            ORDER BY block_date ASC
        ) AS prev_block_date
    FROM
        recent_data
)
SELECT
    contract_address,
    address,
    prev_block_date,
    block_date,
    DATEDIFF(day, prev_block_date, block_date) - 1 AS gap_days
FROM
    source
WHERE
    DATEDIFF(day, prev_block_date, block_date) > 1
ORDER BY
    gap_days DESC

{% endtest %}

