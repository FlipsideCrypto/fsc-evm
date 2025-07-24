{% macro unverify_balances() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
    {% if model.name == 'silver__balance_slots' %}
        DELETE FROM {{ this }} 
        WHERE contract_address NOT IN (
            SELECT token_address AS contract_address
            FROM {{ ref('price__ez_asset_metadata') }}
            WHERE
                is_verified
                AND asset_id IS NOT NULL
                AND token_address IS NOT NULL
        );
    {% elif model.name == 'balances__ez_balances_erc20' %}
        DELETE FROM {{ this }} 
        WHERE contract_address NOT IN (
            SELECT contract_address 
            FROM {{ ref('silver__balance_slots') }}
            WHERE slot_number IS NOT NULL 
            AND num_slots = 1
        );
    {% endif %}
  {% endif %}
{% endmacro %}