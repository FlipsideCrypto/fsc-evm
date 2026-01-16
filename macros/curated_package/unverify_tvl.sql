{% macro unverify_tvl() %}
  {% if var('HEAL_MODEL', false) and is_incremental() %}
        /*
        Delete TVL rows that no longer have a corresponding reads record.

        Only applies to platforms that use the verified_check_enabled system.
        Reads records can be removed upstream by unverify_contract_reads() when tokens lose verification,
        but high-value pools (verified_check_enabled='false') are protected and kept.

        TVL structure varies by version:
          - v2 style: contract_address=token, address=pool  |  reads: contract_address=pool, address=NULL
          - v3/v4 style: contract_address=token, address=pool  |  reads: contract_address=token, address=pool
        */
        WITH platforms_using_verified_check AS (
            -- Platforms that have at least one record using the verified_check system
            SELECT DISTINCT platform
            FROM {{ ref('streamline__contract_reads_records') }}
            WHERE metadata:verified_check_enabled::STRING = 'true'
        ),
        valid_v2_pools AS (
            -- v2 style: reads record has pool as contract_address, address is NULL
            SELECT contract_address AS pool_address, platform
            FROM {{ ref('streamline__contract_reads_records') }}
            WHERE address IS NULL
        ),
        valid_v3_v4_combos AS (
            -- v3/v4 style: reads record has token as contract_address, pool as address
            SELECT contract_address AS token_address, address AS pool_address, platform
            FROM {{ ref('streamline__contract_reads_records') }}
            WHERE address IS NOT NULL
        )
        DELETE FROM {{ this }} t
        WHERE t.platform IN (SELECT platform FROM platforms_using_verified_check)
          AND NOT (
              -- Keep if valid in v2 style (t.address is the pool)
              EXISTS (
                  SELECT 1 FROM valid_v2_pools v
                  WHERE v.pool_address = t.address
                    AND v.platform = t.platform
              )
              OR
              -- Keep if valid in v3/v4 style (t.contract_address is token, t.address is pool)
              EXISTS (
                  SELECT 1 FROM valid_v3_v4_combos v
                  WHERE v.token_address = t.contract_address
                    AND v.pool_address = t.address
                    AND v.platform = t.platform
              )
          );
  {% endif %}
{% endmacro %}