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
    s USING (contract_address)
WHERE
    slot_number IS NULL 
{% endtest %}
