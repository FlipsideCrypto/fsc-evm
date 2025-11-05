{% macro unverify_balances() %}
{% set vars = return_vars() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
    {% if model.name == 'balances__ez_balances_erc20' %}
        DELETE FROM {{ this }} 
        WHERE contract_address NOT IN (
            SELECT contract_address 
            FROM {{ ref('silver__balance_slots') }}
            WHERE slot_number IS NOT NULL 
            AND num_slots = 1
        );
    {% else %}
        DELETE FROM {{ this }} t
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ ref('price__ez_asset_metadata') }} m
            WHERE
                m.token_address = t.contract_address
                AND m.is_verified
                AND m.asset_id IS NOT NULL
                AND m.token_address IS NOT NULL
        )
        OR EXISTS (
          SELECT 1
          FROM {{ ref('silver__balances_erc20_override') }} o
          WHERE o.contract_address = t.contract_address
          AND o.blockchain = '{{ vars.GLOBAL_PROJECT_NAME }}'
        );
    {% endif %}
  {% endif %}
{% endmacro %}