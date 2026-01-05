{% macro unverify_contract_reads() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        DELETE FROM {{ this }} 
        WHERE metadata:verified_check_enabled::STRING = 'true'
        AND (
            metadata:token0::STRING NOT IN (
                SELECT token_address
                FROM {{ ref('price__ez_asset_metadata') }}
                WHERE is_verified AND token_address IS NOT NULL
            )
            OR metadata:token1::STRING NOT IN (
                SELECT token_address
                FROM {{ ref('price__ez_asset_metadata') }}
                WHERE is_verified AND token_address IS NOT NULL
            )
        );
  {% endif %}
{% endmacro %}
