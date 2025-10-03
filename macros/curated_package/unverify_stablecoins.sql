{% macro unverify_stablecoins() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        DELETE FROM {{ this }} 
        WHERE token_address NOT IN (
            SELECT token_address
            FROM {{ ref('price__ez_asset_metadata') }}
            WHERE
                is_verified
                AND asset_id IS NOT NULL
                AND token_address IS NOT NULL
        );
  {% endif %}
{% endmacro %}