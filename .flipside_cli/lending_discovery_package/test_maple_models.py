#!/usr/bin/env python3
"""
Test Maple Finance ERC4626 models against Ethereum database
"""
import snowflake.connector

# Connection config - uses externalbrowser auth
conn_params = {
    'account': 'vna27887.us-east-1',
    'user': 'mattromano@flipsidecrypto.com',
    'authenticator': 'externalbrowser',
    'role': 'INTERNAL_DEV',
    'database': 'ETHEREUM',
    'warehouse': 'DBT',
    'schema': 'silver'
}

# Maple pool manager addresses
POOL_MANAGERS = [
    '0x7ad5ffa5fdf509e30186f4609c2f6269f4b6158f',
    '0x0cda32e08b48bfddbc7ee96b44b09cf286f9e21a',
    '0x9cef7d1d390a4811bba1bc40a53b40a506c33b19',
    '0x5ee9587bf5f4ccceeed87b0216a31ebb513fac25',
    '0xa9c908ee077ee26b52137fff714150c7eb69e160'
]

# Test queries
QUERIES = {
    "0_check_dim_contracts_schema": """
    -- Check schema of dim_contracts
    SELECT column_name, data_type
    FROM ETHEREUM.information_schema.columns
    WHERE table_schema = 'CORE' AND table_name = 'DIM_CONTRACTS'
    ORDER BY ordinal_position
    """,

    "1_find_maple_vaults": f"""
    -- Find vault contracts created by Maple pool managers
    SELECT
        c.address AS vault_address,
        c.creator_address AS pool_manager,
        c.created_block_number,
        c.created_block_timestamp,
        c.name AS vault_name,
        c.symbol AS vault_symbol,
        c.decimals AS vault_decimals
    FROM
        ETHEREUM.core.dim_contracts c
    WHERE
        c.creator_address IN ({','.join([f"'{addr}'" for addr in POOL_MANAGERS])})
        AND c.decimals IS NOT NULL
    ORDER BY c.created_block_number DESC
    LIMIT 20
    """,

    "2_deposit_events_count": f"""
    -- Count deposit events for Maple vaults
    WITH maple_vaults AS (
        SELECT address AS vault_address
        FROM ETHEREUM.core.dim_contracts
        WHERE creator_address IN ({','.join([f"'{addr}'" for addr in POOL_MANAGERS])})
        AND decimals IS NOT NULL
    )
    SELECT
        COUNT(*) as total_deposits,
        COUNT(DISTINCT contract_address) as unique_vaults,
        MIN(block_timestamp) as first_deposit,
        MAX(block_timestamp) as last_deposit
    FROM ETHEREUM.core.fact_event_logs
    WHERE topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
    AND contract_address IN (SELECT vault_address FROM maple_vaults)
    """,

    "3_sample_deposits": f"""
    -- Sample deposit events with parsed data
    WITH maple_vaults AS (
        SELECT
            address AS vault_address,
            name AS vault_name,
            symbol AS vault_symbol
        FROM ETHEREUM.core.dim_contracts
        WHERE creator_address IN ({','.join([f"'{addr}'" for addr in POOL_MANAGERS])})
        AND decimals IS NOT NULL
    )
    SELECT
        l.block_timestamp,
        l.tx_hash,
        l.contract_address AS vault_address,
        mv.vault_symbol,
        CONCAT('0x', SUBSTR(l.topics[1]::STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(l.topics[2]::STRING, 27, 40)) AS owner,
        regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{{64}}') AS segmented_data,
        TRY_TO_NUMBER(ETHEREUM.PUBLIC.udf_hex_to_int(regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{{64}}')[0]::STRING)) AS assets_raw,
        TRY_TO_NUMBER(ETHEREUM.PUBLIC.udf_hex_to_int(regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{{64}}')[1]::STRING)) AS shares_raw
    FROM ETHEREUM.core.fact_event_logs l
    JOIN maple_vaults mv ON l.contract_address = mv.vault_address
    WHERE l.topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
    AND l.block_timestamp >= CURRENT_DATE - 30
    ORDER BY l.block_timestamp DESC
    LIMIT 10
    """,

    "4_withdrawal_events_count": f"""
    -- Count withdrawal events for Maple vaults
    WITH maple_vaults AS (
        SELECT address AS vault_address
        FROM ETHEREUM.core.dim_contracts
        WHERE creator_address IN ({','.join([f"'{addr}'" for addr in POOL_MANAGERS])})
        AND decimals IS NOT NULL
    )
    SELECT
        COUNT(*) as total_withdrawals,
        COUNT(DISTINCT contract_address) as unique_vaults,
        MIN(block_timestamp) as first_withdrawal,
        MAX(block_timestamp) as last_withdrawal
    FROM ETHEREUM.core.fact_event_logs
    WHERE topics[0]::STRING = '0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db'
    AND contract_address IN (SELECT vault_address FROM maple_vaults)
    """,

    "5_get_underlying_assets": f"""
    -- Get underlying asset for each vault from first deposit
    WITH maple_vaults AS (
        SELECT
            address AS vault_address,
            name AS vault_name,
            symbol AS vault_symbol
        FROM ETHEREUM.core.dim_contracts
        WHERE creator_address IN ({','.join([f"'{addr}'" for addr in POOL_MANAGERS])})
        AND decimals IS NOT NULL
    ),
    first_deposits AS (
        SELECT
            l.contract_address AS vault_address,
            CONCAT('0x', SUBSTR(l.topics[1]::STRING, 27, 40)) AS potential_underlying,
            ROW_NUMBER() OVER (PARTITION BY l.contract_address ORDER BY l.block_number ASC) AS rn
        FROM ETHEREUM.core.fact_event_logs l
        WHERE l.topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
        AND l.contract_address IN (SELECT vault_address FROM maple_vaults)
    )
    SELECT
        mv.vault_address,
        mv.vault_name,
        mv.vault_symbol,
        c.address AS underlying_address,
        c.symbol AS underlying_symbol,
        c.decimals AS underlying_decimals
    FROM maple_vaults mv
    LEFT JOIN first_deposits fd ON mv.vault_address = fd.vault_address AND fd.rn = 1
    LEFT JOIN ETHEREUM.core.dim_contracts c ON fd.potential_underlying = c.address
    """
}

def run_tests():
    print("Connecting to Snowflake (will open browser for auth)...")
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()

    print("\n" + "="*60)
    print("MAPLE FINANCE ERC4626 MODEL VALIDATION")
    print("="*60)

    for name, query in QUERIES.items():
        print(f"\n{'='*60}")
        print(f"TEST: {name}")
        print("="*60)
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            print(f"Columns: {columns}")
            print(f"Row count: {len(results)}")
            print("-"*40)

            for i, row in enumerate(results[:10]):  # Show first 10 rows
                print(f"Row {i+1}: {dict(zip(columns, row))}")

            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")

        except Exception as e:
            print(f"ERROR: {e}")

    cursor.close()
    conn.close()
    print("\n" + "="*60)
    print("TESTS COMPLETE")
    print("="*60)

if __name__ == "__main__":
    run_tests()
