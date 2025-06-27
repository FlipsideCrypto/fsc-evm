{% test missing_balance_slots(
    model
) %}
SELECT
    contract_address,
    slot_number,
    num_slots
FROM
    {{ ref('price__ez_asset_metadata') }}
    v
    LEFT JOIN {{ ref('silver__balance_slots') }}
    s
    ON v.token_address = s.contract_address
WHERE
    slot_number IS NULL
    AND is_verified
    AND asset_id IS NOT NULL
    AND token_address IS NOT NULL 
{% endtest %}
