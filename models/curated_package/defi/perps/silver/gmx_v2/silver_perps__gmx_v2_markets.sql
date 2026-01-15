{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'market_address',
    cluster_by = ['first_seen_timestamp::DATE'],
    tags = ['silver_perps','defi','perps','curated','gmx','dim']
) }}

{#
GMX v2 Markets Dimension Table
Derives market information from position events and uses hardcoded mappings
for known GMX v2 market addresses (GM token addresses).

Market addresses are the GMX v2 GM pool token addresses.
#}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_DEX_SWAPS_CONTRACT_MAPPING
    ) }}
    WHERE
        protocol = 'gmx'
        AND version = 'v2'
),

-- Known GMX v2 markets on Arbitrum (GM pool tokens)
-- Source: https://docs.gmx.io/docs/api/markets and on-chain analysis
known_markets AS (
    SELECT column1 AS market_address, column2 AS market_name, column3 AS index_token_symbol
    FROM (VALUES
        -- Top markets by trading volume (from on-chain data)
        ('0x1064b9d788314d6bafd5a318c6f15bd3366b67a6', 'ETH/USD', 'ETH'),
        ('0xe8d8bb57edf69e4daf806ba60bf2658c81eb91db', 'ETH/USD', 'ETH'),
        ('0xfb524b1dc4ad6acc100952e921a298dcc87a7d53', 'BTC/USD', 'BTC'),
        ('0x3528c0ddbdaebc4c0eefa9a09abde7f986539494', 'XRP/USD', 'XRP'),
        ('0xdeb279407e3d3e4a0fdd53b18b70d7e6ceb77d9b', 'ETH/USD', 'ETH'),
        ('0x5c83942b7919db30634f9bc9e0e72ad778852fc8', 'LINK/USD', 'LINK'),
        ('0x6c89360e71790cbfc81da69caeda5bdc6bffc0a1', 'UNI/USD', 'UNI'),
        ('0xc2e204d9dc2a82f238a138d5c581fd297ee74f49', 'PEPE/USD', 'PEPE'),
        ('0xb2426756573c680b9ccd03418c5641faf3b70684', 'WIF/USD', 'WIF'),
        ('0x1640e916e10610ba39aac5cd8a08acf3ccae1a4c', 'NEAR/USD', 'NEAR'),
        ('0x4058b351477733aa62843fb596b13a631b21ee26', 'ATOM/USD', 'ATOM'),
        ('0x40878651550b20197831b9a4c3d17b8c8c66a54d', 'AAVE/USD', 'AAVE'),
        ('0xaa0edd4a698a4ac1c4522374eb83a7496ee89532', 'AVAX/USD', 'AVAX'),
        ('0xc7468a9aee4b4623d298a9cf6d43241e1a52d0be', 'DOGE/USD', 'DOGE'),
        ('0x84131a7b00d4c569afc904efef170a2e5e43a1e8', 'STX/USD', 'STX'),
        ('0x8323913c6faaa1f9620ac98b2be12262d9d3b816', 'OP/USD', 'OP'),
        ('0x05d2db9dccdeaa94a4a7b3e14a78eeaf58639cd7', 'LTC/USD', 'LTC'),
        ('0xd0283cc5f2e617b06aae8e4dfcca85dae828a96c', 'BNB/USD', 'BNB'),
        ('0xc90970503b310063c3837eb37640b87d13f1bd9d', 'ARB/USD', 'ARB'),
        ('0xd248e822a1684bb696f087fd3d3f772a0ae63260', 'GMX/USD', 'GMX'),
        ('0x1b20bd8b6c01f55f5e421f505519f371b4e605c4', 'SHIB/USD', 'SHIB'),
        ('0x7bdc57a20bea89f99ca021f1cfc8d9ed772d5415', 'SUI/USD', 'SUI'),
        ('0x55499bd0eeda54632996f40bdbcca5f6e1468619', 'APE/USD', 'APE'),
        ('0xd05ac153b166f7760c1dcbc13230923a93a14a7e', 'ORDI/USD', 'ORDI'),
        ('0x38e4e611a9565eb5128d8fbec5d03dd68b099258', 'SOL/USD', 'SOL'),
        ('0xf4876de9e84040695f40b8b010788958bf106a62', 'BONK/USD', 'BONK'),
        ('0xe295fe001aea1d3a6efb61e9e171f57da56a372e', 'POL/USD', 'POL'),
        ('0xb75927b6440f0e921bdace531afa4ab725769e03', 'PENDLE/USD', 'PENDLE'),
        ('0x1d652df2a24cf2c6c6b7f3e71fb95b094500b85a', 'ENA/USD', 'ENA'),
        ('0x186645fafdd6d3de2d819596d5891341b2c57c32', 'SOL/USD', 'SOL'),
        -- Additional documented markets
        ('0x70d95587d40a2caf56bd97485ab3eec10bee6336', 'ETH/USD', 'ETH'),
        ('0x450bb6774dd8a756274e0ab4107953259d2ac541', 'ETH/USD', 'ETH'),
        ('0x6853ea96ff216fab11d2d930ce3c508556a4bdc4', 'BTC/USD', 'BTC'),
        ('0x47c031236e19d024b42f8ae6780e44a573170703', 'BTC/USD', 'BTC'),
        ('0x09400d9db990d5ed3f35d7be61dfaeb900af03c9', 'SOL/USD', 'SOL'),
        ('0xc25cef6061cf5de5eb761b50e4743c1f5d7e5407', 'ARB/USD', 'ARB'),
        ('0x7f1fa204bb700853d36994da19f830b6ad18455c', 'LINK/USD', 'LINK'),
        ('0xc7abb2c5f3bf3ceb389df0eecd6120d451170b50', 'UNI/USD', 'UNI'),
        ('0x339eF6aAcF8F4B2AD15BdcECBEED1842Ec4dBcBf', 'AAVE/USD', 'AAVE'),
        ('0xd62068697bCc92AF253225676D618B0C9f17C663', 'AVAX/USD', 'AVAX'),
        ('0xb7e69749E3d2EDd90ea59A4932EFEa2D41E245d7', 'NEAR/USD', 'NEAR'),
        ('0x0CCB4fAa6f1F1B30911619f1184082aB4E25813c', 'ATOM/USD', 'ATOM'),
        ('0xB686BcB112660343E6d15BDb65297e110C8311c4', 'OP/USD', 'OP'),
        ('0xD9535bB5f58A1a75032416F2dFe7880C30575a41', 'DOGE/USD', 'DOGE'),
        ('0x63Dc80EE90F26363B3FCD609007CC9e14c8991BE', 'LTC/USD', 'LTC'),
        ('0x0418643F94Ef14917f1345cE5C460C37dE463ef7', 'XRP/USD', 'XRP'),
        ('0x2b477989A149B17073D9C9C82eC9cB03591e20c6', 'GMX/USD', 'GMX'),
        ('0x55391D178Ce46e7AC8eaAEa50A72D1A5a8A622Da', 'PEPE/USD', 'PEPE'),
        ('0x248C35760068cE009a13076D573ed3497A47bCD4', 'WIF/USD', 'WIF'),
        ('0x7BbBf946883a5701350007320F525c5379B8178A', 'STX/USD', 'STX')
    )
),

-- Extract unique markets from position events
position_markets AS (
    SELECT
        decoded_log:eventData[0][0][0][1]::STRING AS market_address,
        MIN(block_timestamp) AS first_seen_timestamp,
        MIN(tx_hash) AS first_seen_tx_hash,
        MAX(modified_timestamp) AS modified_timestamp
    FROM {{ ref('core__ez_decoded_event_logs') }} l
    INNER JOIN contract_mapping m
        ON l.contract_address = m.contract_address
    WHERE
        decoded_log:eventName::STRING IN ('PositionIncrease', 'PositionDecrease')
        AND tx_succeeded
        AND decoded_log:eventData[0][0][0][1]::STRING IS NOT NULL
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
    {% endif %}
    GROUP BY 1
),

-- Enrich with known market metadata
enriched_markets AS (
    SELECT
        p.market_address,
        COALESCE(k.market_name, 'UNKNOWN/USD') AS market_name,
        COALESCE(k.index_token_symbol, 'UNKNOWN') AS index_token_symbol,
        p.first_seen_timestamp,
        p.first_seen_tx_hash,
        'gmx-v2' AS platform,
        'gmx' AS protocol,
        'v2' AS version,
        p.modified_timestamp
    FROM position_markets p
    LEFT JOIN known_markets k
        ON LOWER(p.market_address) = LOWER(k.market_address)
)

SELECT
    market_address,
    market_name,
    index_token_symbol,
    first_seen_timestamp,
    first_seen_tx_hash,
    platform,
    protocol,
    version,
    modified_timestamp
FROM enriched_markets
QUALIFY ROW_NUMBER() OVER (PARTITION BY market_address ORDER BY modified_timestamp DESC) = 1
