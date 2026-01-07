{% macro unverify_contract_reads() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        DELETE FROM {{ this }} t
        WHERE t.metadata:verified_check_enabled::STRING = 'true'
        AND (
            (
                t.metadata:token0::STRING IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM {{ ref('price__ez_asset_metadata') }} v
                    WHERE v.is_verified 
                    AND v.token_address IS NOT NULL
                    AND v.token_address = t.metadata:token0::STRING
                )
            )
            OR (
                t.metadata:token1::STRING IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM {{ ref('price__ez_asset_metadata') }} v
                    WHERE v.is_verified 
                    AND v.token_address IS NOT NULL
                    AND v.token_address = t.metadata:token1::STRING
                )
            )
        );
  {% endif %}
{% endmacro %}
