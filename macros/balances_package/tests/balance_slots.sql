{% test balance_slots(model) %}

{# If > 1 slots in array or NULL, then balance slot is missing. 
Logic for these contracts must be handled separately (e.g. rebase tokens) #}

    SELECT
        contract_address,
        ARRAY_AGG(DISTINCT slot_number) AS slot_number_array,
        COUNT(DISTINCT slot_number) AS distinct_slots,
        COUNT(CASE WHEN slot_number IS NULL THEN 1 END) AS null_slots
    FROM {{ model }}
    GROUP BY contract_address
    HAVING COUNT(DISTINCT slot_number) > 1 
        OR COUNT(CASE WHEN slot_number IS NULL THEN 1 END) > 0

{% endtest %}