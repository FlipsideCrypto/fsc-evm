#!/usr/bin/env python3
"""
Final validation of Maple Finance ERC4626 models
"""
import snowflake.connector

conn_params = {
    'account': 'vna27887.us-east-1',
    'user': 'mattromano@flipsidecrypto.com',
    'authenticator': 'externalbrowser',
    'role': 'INTERNAL_DEV',
    'database': 'ETHEREUM',
    'warehouse': 'DBT',
    'schema': 'silver'
}

POOL_MANAGERS = [
    '0x7ad5ffa5fdf509e30186f4609c2f6269f4b6158f',
    '0x0cda32e08b48bfddbc7ee96b44b09cf286f9e21a',
    '0x9cef7d1d390a4811bba1bc40a53b40a506c33b19',
    '0x5ee9587bf5f4ccceeed87b0216a31ebb513fac25',
    '0xa9c908ee077ee26b52137fff714150c7eb69e160'
]

FINAL_TEST = f"""
-- Full model simulation: maple_pools -> maple_deposits -> complete -> ez
WITH pool_managers AS (
    SELECT * FROM (VALUES
        ('0x7ad5ffa5fdf509e30186f4609c2f6269f4b6158f', 'maple', 'v2', 'erc4626_pool_managers'),
        ('0x0cda32e08b48bfddbc7ee96b44b09cf286f9e21a', 'maple', 'v2', 'erc4626_pool_managers'),
        ('0x9cef7d1d390a4811bba1bc40a53b40a506c33b19', 'maple', 'v2', 'erc4626_pool_managers'),
        ('0x5ee9587bf5f4ccceeed87b0216a31ebb513fac25', 'maple', 'v2', 'erc4626_pool_managers'),
        ('0xa9c908ee077ee26b52137fff714150c7eb69e160', 'maple', 'v2', 'erc4626_pool_managers')
    ) AS t(contract_address, protocol, version, type)
),

-- silver_erc4626__maple_pools equivalent
vault_contracts AS (
    SELECT
        c.address AS vault_address,
        c.creator_address AS pool_manager,
        c.created_block_number,
        c.created_block_timestamp,
        c.name AS vault_name,
        c.symbol AS vault_symbol,
        c.decimals AS vault_decimals,
        pm.protocol,
        pm.version,
        pm.protocol || '-' || pm.version AS platform,
        c.modified_timestamp
    FROM ETHEREUM.core.dim_contracts c
    INNER JOIN pool_managers pm ON c.creator_address = pm.contract_address
    WHERE c.decimals IS NOT NULL
),

underlying_mapping AS (
    SELECT
        vc.vault_address,
        vc.vault_symbol,
        CASE
            WHEN UPPER(vc.vault_symbol) LIKE '%USDC%' THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            WHEN UPPER(vc.vault_symbol) LIKE '%USDT%' THEN '0xdac17f958d2ee523a2206206994597c13d831ec7'
            ELSE NULL
        END AS underlying_asset_address
    FROM vault_contracts vc
),

pools AS (
    SELECT
        vc.vault_address,
        vc.vault_name,
        vc.vault_symbol,
        vc.vault_decimals,
        um.underlying_asset_address,
        c.symbol AS underlying_symbol,
        c.decimals AS underlying_decimals,
        vc.protocol,
        vc.version,
        vc.platform
    FROM vault_contracts vc
    LEFT JOIN underlying_mapping um ON vc.vault_address = um.vault_address
    LEFT JOIN ETHEREUM.core.dim_contracts c ON um.underlying_asset_address = c.address
),

-- silver_erc4626__maple_deposits equivalent
deposit_events AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address AS vault_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{{64}}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics[2]::STRING, 27, 40)) AS owner,
        TRY_TO_NUMBER(ETHEREUM.PUBLIC.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{{64}}')[0]::STRING)) AS assets_raw,
        TRY_TO_NUMBER(ETHEREUM.PUBLIC.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{{64}}')[1]::STRING)) AS shares_raw,
        l.modified_timestamp,
        CONCAT(l.tx_hash::STRING, '-', l.event_index::STRING) AS _log_id
    FROM ETHEREUM.core.fact_event_logs l
    WHERE l.topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
    AND l.contract_address IN (SELECT vault_address FROM pools)
    AND l.tx_succeeded
    AND l.block_timestamp >= CURRENT_DATE - 30
),

maple_deposits AS (
    SELECT
        d.tx_hash,
        d.block_number,
        d.block_timestamp,
        d.event_index,
        d.origin_from_address,
        d.origin_to_address,
        d.origin_function_signature,
        d.vault_address,
        d.sender,
        d.owner AS depositor,
        d.assets_raw AS amount_unadj,
        d.shares_raw AS shares_unadj,
        p.vault_address AS protocol_market,
        p.vault_symbol AS protocol_market_symbol,
        p.underlying_asset_address AS token_address,
        p.underlying_symbol AS token_symbol,
        p.underlying_decimals AS token_decimals,
        p.protocol,
        p.version,
        p.platform,
        d._log_id,
        d.modified_timestamp,
        'Deposit' AS event_name
    FROM deposit_events d
    LEFT JOIN pools p ON d.vault_address = p.vault_address
),

-- Join with prices for complete model
prices AS (
    SELECT token_address, price, HOUR
    FROM ETHEREUM.price.ez_prices_hourly
    WHERE token_address IN ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', '0xdac17f958d2ee523a2206206994597c13d831ec7')
),

complete_deposits AS (
    SELECT
        d.tx_hash,
        d.block_number,
        d.block_timestamp,
        d.event_index,
        d.origin_from_address,
        d.origin_to_address,
        d.protocol_market,
        d.protocol_market_symbol,
        d.depositor,
        d.token_address,
        d.token_symbol,
        d.amount_unadj,
        d.amount_unadj / pow(10, d.token_decimals) AS amount,
        d.shares_unadj,
        d.shares_unadj / pow(10, 18) AS shares,
        ROUND((d.amount_unadj / pow(10, d.token_decimals)) * p.price, 2) AS amount_usd,
        d.platform,
        d.protocol,
        d.version,
        d.event_name
    FROM maple_deposits d
    LEFT JOIN prices p ON d.token_address = p.token_address
        AND DATE_TRUNC('hour', d.block_timestamp) = p.hour
)

SELECT
    block_timestamp,
    tx_hash,
    protocol_market_symbol,
    depositor,
    token_symbol,
    amount,
    amount_usd,
    platform
FROM complete_deposits
ORDER BY block_timestamp DESC
LIMIT 15
"""

def run_test():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("FINAL MODEL VALIDATION - SIMULATING FULL PIPELINE")
    print("="*80)

    try:
        cursor.execute(FINAL_TEST)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        print(f"\nColumns: {columns}")
        print(f"Row count: {len(results)}")
        print("-"*80)

        for i, row in enumerate(results):
            row_dict = dict(zip(columns, row))
            print(f"\nRow {i+1}:")
            for k, v in row_dict.items():
                print(f"  {k}: {v}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    cursor.close()
    conn.close()
    print("\n" + "="*80)
    print("VALIDATION COMPLETE")
    print("="*80)

if __name__ == "__main__":
    run_test()
