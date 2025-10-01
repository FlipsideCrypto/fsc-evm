{% macro remove_test_tokens() %}
  {% if var('HEAL_MODEL', false) %}
      {% if 'liquidations' in this.identifier %}
      DELETE FROM {{ this }}
      WHERE (collateral_token NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      )
      OR debt_token NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      ));
      {% else %}
      DELETE FROM {{ this }}
      WHERE token_address NOT IN (
          SELECT contract_address
          FROM {{ ref('silver__relevant_contracts') }}
          WHERE total_event_count > 250
      );
      {% endif %}
  {% endif %}
{% endmacro %}