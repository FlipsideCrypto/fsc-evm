{% macro unverify_tvl() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        -- Only target platforms that use verified_check_enabled
        -- Delete rows that are invalid in BOTH v2 and v3/v4 styles
        DELETE FROM {{ this }} t
        WHERE EXISTS (
            SELECT 1 
            FROM {{ ref('streamline__contract_reads_records') }}
            WHERE metadata:verified_check_enabled::STRING = 'true'
            AND platform = t.platform
        ) --necessary for the complete_tvl model
        AND 
            -- Not valid in v2 style (address as pool)
            NOT EXISTS (
                SELECT 1
                FROM {{ ref('streamline__contract_reads_records') }} r
                WHERE r.metadata:verified_check_enabled::STRING = 'true'
                AND r.address IS NULL
                AND r.contract_address = t.address
            )
        AND
            -- Not valid in v3/v4 style (token-address combo)
            NOT EXISTS (
                SELECT 1
                FROM {{ ref('streamline__contract_reads_records') }} r
                WHERE r.metadata:verified_check_enabled::STRING = 'true'
                AND r.address IS NOT NULL
                AND r.contract_address = t.contract_address
                AND r.address = t.address
            );
  {% endif %}
{% endmacro %}