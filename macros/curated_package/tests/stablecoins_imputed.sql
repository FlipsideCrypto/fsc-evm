{% test stablecoins_is_imputed_false(model, lookback_days=14) %}

WITH active_pairs AS (
    SELECT DISTINCT
        contract_address,
        address
    FROM
        {{ model }}
    WHERE
        block_date >= DATEADD('day', -{{ lookback_days }}, SYSDATE())
        AND not is_imputed
),
first_records AS (
    SELECT
        m.contract_address,
        m.address,
        m.is_imputed,
        ROW_NUMBER() OVER (
            PARTITION BY m.contract_address, m.address
            ORDER BY m.block_date ASC
        ) AS row_num
    FROM {{ model }} m
    INNER JOIN active_pairs a
        ON m.contract_address = a.contract_address
        AND m.address = a.address
)
SELECT
    contract_address,
    address,
    is_imputed
FROM first_records
WHERE row_num = 1
    AND is_imputed

{% endtest %}

