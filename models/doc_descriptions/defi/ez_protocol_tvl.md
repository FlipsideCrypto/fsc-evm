{% docs ez_protocol_tvl_table_doc %}

## What

This table provides daily Total Value Locked (TVL) metrics for DeFi protocols across EVM-compatible blockchains. It aggregates values by platform to track liquidity and value deposited in various DeFi applications. Methods may vary by protocol and blockchain.

## Methodology Note

For certain protocols (e.g., Uniswap v2/v3/v4 and forks), TVL is calculated only for pools where both tokens are verified. This filtering removes low-quality or spam pools, providing a more accurate representation of "real" TVL rather than values inflated by low-liquidity tokens.

## Key Use Cases

- Tracking protocol TVL growth and trends over time
- Comparing TVL across different protocols and platforms
- Analyzing protocol adoption and liquidity depth
- Building TVL-based dashboards and metrics

## Commonly-used Fields

- `block_date`: Date of the TVL snapshot
- `tvl_usd`: Total Value Locked in USD
- `protocol`: Name of the DeFi protocol (e.g., Uniswap, Aave)
- `version`: Protocol version (e.g., v2, v3)
- `platform`: Specific deployment or platform identifier

## Sample queries

```sql
-- Latest TVL by protocol
SELECT 
    protocol,
    version,
    tvl_usd
FROM <blockchain_name>.defi.ez_protocol_tvl
WHERE block_date = CURRENT_DATE - 1
ORDER BY tvl_usd DESC;

-- Daily TVL trend for a specific protocol in aggregate
SELECT 
    block_date,
    SUM(tvl_usd) AS tvl_usd_total
FROM <blockchain_name>.defi.ez_protocol_tvl
WHERE protocol = 'uniswap'
    AND block_date >= CURRENT_DATE - 30
GROUP BY block_date, protocol
ORDER BY block_date DESC;

-- TVL comparison across platforms
SELECT 
    block_date,
    platform,
    SUM(tvl_usd) AS total_tvl
FROM <blockchain_name>.defi.ez_protocol_tvl
WHERE block_date >= CURRENT_DATE - 7
GROUP BY block_date, platform
ORDER BY block_date DESC, total_tvl DESC;
```

{% enddocs %}

{% docs ez_protocol_tvl_block_date %}

The date of the daily TVL snapshot.

Example: '2025-06-10'

{% enddocs %}

{% docs ez_protocol_tvl_tvl_usd %}

The total value locked in USD for the protocol on the given date. Values exceeding $1 trillion (1e12) are excluded to filter out erroneous pricing data.

Example: 1500000000

{% enddocs %}

{% docs ez_protocol_tvl_protocol %}

The name of the DeFi protocol.

Example: 'Uniswap'

{% enddocs %}

{% docs ez_protocol_tvl_version %}

The version of the protocol deployment.

Example: 'v3'

{% enddocs %}

{% docs ez_protocol_tvl_platform %}

The specific platform or deployment identifier for the protocol.

Example: 'uniswap-v3-ethereum'

{% enddocs %}

