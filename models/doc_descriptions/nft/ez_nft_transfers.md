{% docs ez_nft_transfers_table_doc %}

## Table: ez_nft_transfers

This table contains all NFT transfer events for ERC-721 and ERC-1155 tokens on EVM blockchains. It provides a comprehensive view of NFT movements including transfers, mints, and burns, with enriched metadata for easier analysis.

### Key Features:
- **Complete NFT Movement**: All ERC-721 and ERC-1155 transfers in one table
- **Mint Detection**: Identifies minting events (from 0x0 address)
- **Burn Detection**: Identifies burning events (to 0x0 address)
- **Batch Support**: Handles ERC-1155 batch transfers
- **Metadata Enrichment**: Includes collection names and project details where available

### Token Standards Covered:
| Standard | Description | Quantity Support |
|----------|-------------|------------------|
| ERC-721 | Non-fungible tokens | Always 1 |
| ERC-1155 | Multi-token standard | 1 or more |

### Important Relationships:
- **Join with ez_nft_sales**: Use `tx_hash` to match with sales but note that a single transaction can contain multiple sales. Do not use `event_index` to match as the `event_index` in ez_nft_transfers represent the `event_index` of the transfer and not the sale. 
- **Join with dim_nft_collection_metadata**: Use `contract_address` and `token_id` for metadata like traits, token id name and token id description
- **Join with fact_transactions**: Use `tx_hash` for transaction context

### Sample Queries:

**Daily NFT Activity Overview**
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS day,
    COUNT(*) AS total_transfers,
    COUNT(DISTINCT contract_address) AS unique_collections,
    COUNT(DISTINCT CASE WHEN is_mint THEN tx_hash END) AS mint_count,
    COUNT(DISTINCT from_address) AS unique_senders,
    COUNT(DISTINCT to_address) AS unique_receivers,
    SUM(IFF(token_standard = 'erc721', 1 , 0)) as erc721_transfer_count,
    SUM(IFF(token_standard = 'erc1155', 1 , 0)) as erc1155_transfer_count
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 1 DESC;
```

**Popular NFT Collections by Transfer Volume**
```sql
SELECT 
    contract_address,
    name,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT token_id) AS unique_tokens,
    COUNT(DISTINCT from_address) AS unique_senders,
    COUNT(DISTINCT to_address) AS unique_receivers,
    SUM(CASE WHEN is_mint THEN 1 ELSE 0 END) AS mints,
    SUM(CASE WHEN to_address = '0x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) AS burns
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
    AND name IS NOT NULL 
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 50;
```
- Only use this filter `AND name IS NOT NULL` when you want collections with known names but note that most collections do not have a name. It is best to exclude this filter. 

**NFT Minting Analysis**
```sql
SELECT 
    contract_address,
    name,
    DATE_TRUNC('hour', block_timestamp) AS mint_hour,
    COUNT(*) AS mint_count,
    COUNT(DISTINCT to_address) AS unique_minters,
    COUNT(DISTINCT token_id) as unique_token_id_count 
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE is_mint = TRUE
    AND block_timestamp >= CURRENT_DATE - 1
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```

**Wallet NFT Activity**
```sql
WITH wallet_activity AS (
    SELECT 
        address,
        SUM(received) AS nfts_received,
        SUM(sent) AS nfts_sent,
        SUM(received) - SUM(sent) AS net_change,
        COUNT(DISTINCT contract_address) AS collections_interacted
    FROM (
        SELECT to_address AS address, COUNT(*) AS received, 0 AS sent, contract_address
        FROM <blockchain_name>.nft.ez_nft_transfers
        WHERE block_timestamp >= CURRENT_DATE - 30
            AND to_address != '0x0000000000000000000000000000000000000000'
        GROUP BY 1, 4
        
        UNION ALL
        
        SELECT from_address AS address, 0 AS received, COUNT(*) AS sent, contract_address
        FROM <blockchain_name>.nft.ez_nft_transfers
        WHERE block_timestamp >= CURRENT_DATE - 30
            AND from_address != '0x0000000000000000000000000000000000000000'
        GROUP BY 1, 4
    )
    GROUP BY 1
)
SELECT * FROM wallet_activity
WHERE collections_interacted > 5
ORDER BY net_change DESC
LIMIT 100;
```

**ERC-1155 Single and Batch Transfer Analysis**
```sql
SELECT 
    contract_address,
    name,
    token_transfer_type, 
    COUNT(*) AS transfer_count 
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
    AND token_transfer_type in (
        'erc1155_TransferSingle',
        'erc1155_TransferBatch'
        )
GROUP BY 1, 2, 3
ORDER BY 4 DESC;
```


### Critical Usage Notes:
- **Token Standards**: Check `token_standard` to distinguish between ERC-721, ERC-1155, legacy and cryptopunks. Legacy tokens and cryptopunks are old nfts that do not adhere to ERC-721 or ERC-1155.
- **Contract Address** Use `contract_address` instead of `nft_address` 
- **Quantity**: ERC-721 always has quantity 1, ERC-1155 uses `quantity`
- **Mint/Burn Detection**: Use `is_mint` flag or check from/to addresses
- **Batch Transfers**: Multiple ERC-1155 token_ids can be transferred in a single `event_index` but is ordered using `intra_event_index`

{% enddocs %}

{% docs ez_nft_transfers_from_address %}

The address sending/transferring the NFT.

**Special Values**:
- `0x0000...0000`: Minting event (NFT creation)
- Contract addresses: Often marketplace or game contracts
- EOA addresses: Individual users

**Query Pattern**:
```sql
-- Find NFT minters
SELECT 
    contract_address,
    to_address AS minters,
    COUNT(*) AS mints
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE is_mint
AND block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 3 DESC;
```

{% enddocs %}

{% docs ez_nft_transfers_to_address %}

The address receiving the NFT.

**Special Values**:
- `0x0000...0000`: Burning event (NFT destruction)
- Contract addresses: Staking, escrow, or marketplace contracts
- EOA addresses: End users

**Analysis Example**:
```sql
-- Identify NFT burns by collection
SELECT 
    contract_address,
    name,
    COUNT(*) AS burns,
    COUNT(DISTINCT token_id) AS unique_tokens_burned
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE to_address = '0x0000000000000000000000000000000000000000'
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2
ORDER BY 3 DESC;
```
{% enddocs %}

{% docs ez_nft_transfers_token_id %}

The unique identifier for a specific NFT within a collection.

**Format**: String to handle various token_id formats
**Standards**:
- ERC-721: Each token_id represents one unique NFT
- ERC-1155: Multiple copies can exist for same token_id

**Common Patterns**:
```sql
-- Count distinct token_id for a collection
SELECT 
    contract_address,
    COUNT(DISTINCT token_id) as unique_token_id 
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE is_mint = TRUE
GROUP BY 1;
```

{% enddocs %}

{% docs ez_nft_transfers_intra_event_index %}

Position within a batch transfer event, primarily for ERC-1155.

**Usage**:
- 1 for single transfers (ERC-721)
- 1,2,3.. for batch transfers (ERC-1155)
- Maintains order within batch operations
- Always start with 1

**Batch Analysis**:
```sql
-- Analyze batch transfer sizes for ERC-1155
SELECT 
    contract_address, 
    token_transfer_type,
    MAX(intra_event_index) AS batch_size,
    COUNT(*) AS occurrence_count
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
AND token_standard = 'erc1155'
GROUP BY 1, 2
ORDER BY 3 DESC;
```

{% enddocs %}

{% docs ez_nft_transfers_nft_quantity %}

The number of NFTs transferred for this specific token_id.

**Standards**:
- ERC-721: Always 1 (non-fungible)
- ERC-1155: Can be 1 or more (semi-fungible)

{% enddocs %}

{% docs ez_nft_transfers_token_transfer_type %}

The specific event type emitted by the contract.

**Values**:
- `erc721_Transfer`: Standard ERC-721 transfer
- `erc1155_TransferSingle`: Single ERC-1155 transfer
- `erc1155_TransferBatch`: Batch ERC-1155 transfer
- `cryptopunks_PunkBought`: Cryptopunks transfer
- `cryptopunks_PunkTransfer`: Cryptopunks transfer
- `legacy_Transfer`: Legacy NFT transfer

**Usage Pattern**:
```sql
-- Distribution of transfer types
SELECT 
    token_transfer_type,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT contract_address) AS unique_collections
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY 1;
```

{% enddocs %}

{% docs ez_nft_transfers_is_mint %}

Boolean flag indicating if this transfer is a minting event.

**Logic**: `from_address = '0x0000000000000000000000000000000000000000'`
**TRUE**: New NFT created
**FALSE**: Existing NFT transferred

**Minting Patterns**:
```sql
-- Daily minting trends
SELECT 
    DATE(block_timestamp) AS mint_date,
    COUNT(DISTINCT contract_address) AS collections_minting,
    COUNT(*) AS total_mints,
    COUNT(DISTINCT to_address) AS unique_minters
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE is_mint = TRUE
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 1 DESC;
```

{% enddocs %}

{% docs ez_nft_transfers_contract_address %}

The address of the contract that emitted the NFT transfer event.

**Usage**:
- Join with dim_contracts to get contract details
- Identify NFT collections by contract address

**Query Example**:
```sql
-- Find NFT collections by contract address
SELECT 
    contract_address,
    COUNT(*) AS total_transfers
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY 2 DESC;
```

{% enddocs %}

{% docs ez_nft_transfers_name %}

The name of the NFT collection.

**Usage**:
- Join with dim_nft_metadata to get collection details
- Identify NFT collections by name

{% enddocs %}

{% docs ez_nft_transfers_token_standard %}

The standard of the NFT. 

**Usage**:
- Identify NFT collections by standard

**Values**:
- ERC-721
- ERC-1155

{% enddocs %}