{% macro remove_test_tokens() %}
  {% if var('HEAL_MODEL', false) %}
      DELETE FROM {{ this }}
      WHERE token_address NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      )
      {% if adapter.get_columns_in_relation(this) | selectattr('name', 'equalto', 'collateral_token') | list | length > 0 %}
      OR collateral_token NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      )
      {% endif %}
      {% if adapter.get_columns_in_relation(this) | selectattr('name', 'equalto', 'debt_token') | list | length > 0 %}
      OR debt_token NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      )
      {% endif %};
  {% endif %}
{% endmacro %}