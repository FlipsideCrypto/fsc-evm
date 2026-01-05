{% macro unverify_tvl() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        -- Only target platforms that use verified_check_enabled
        -- Delete rows that are invalid in BOTH v2 and v3/v4 styles
        DELETE FROM {{ this }} 
        WHERE platform IN (
            SELECT DISTINCT platform 
            FROM {{ ref('streamline__contract_reads_records') }}
            WHERE metadata:verified_check_enabled::STRING = 'true'
        ) --necessary for the complete_tvl model
        AND 
            -- Not valid in v2 style (address as pool)
            address NOT IN (
                SELECT DISTINCT contract_address
                FROM {{ ref('streamline__contract_reads_records') }}
                WHERE metadata:verified_check_enabled::STRING = 'true'
                AND address IS NULL
            )
        AND
            -- Not valid in v3/v4 style (token-address combo)
            CONCAT(contract_address, '-', address) NOT IN (
                SELECT DISTINCT CONCAT(contract_address, '-', address)
                FROM {{ ref('streamline__contract_reads_records') }}
                WHERE metadata:verified_check_enabled::STRING = 'true'
                AND address IS NOT NULL
            );
  {% endif %}
{% endmacro %}