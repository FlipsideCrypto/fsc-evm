{% docs ez_nft_transfers_table_doc %}

## What

This table contains all NFT transfer events for ERC-721 and ERC-1155 tokens on EVM blockchains. It provides a comprehensive view of NFT movements including transfers, mints, and burns, with enriched metadata for easier analysis.

## Key Use Cases

- Track daily NFT activity and transfer volume across collections
- Analyze NFT minting patterns and mint timing
- Identify popular collections by transfer activity
- Monitor wallet NFT accumulation and trading behavior
- Analyze ERC-1155 batch transfer patterns
- Track current NFT holders and ownership changes
- Detect burns and unusual transfer patterns

## Important Relationships

- **Join with ez_nft_sales**: Use `tx_hash` to match with sales but note that a single transaction can contain multiple sales. Do not use `event_index` to match as the `event_index` in ez_nft_transfers represent the `event_index` of the transfer and not the sale
- **Join with dim_nft_collection_metadata**: This is only for the Ethereum blockchain. Use `contract_address` and `token_id` for metadata like traits, token id name and token id description
- **Join with fact_transactions**: Use `tx_hash` for transaction context

## Commonly-used Fields

- `contract_address`: NFT collection contract address
- `token_id`: Unique identifier of the specific NFT
- `from_address` / `to_address`: Transfer participants (0x0 for mint/burn)
- `is_mint`: Boolean flag for minting events
- `token_standard`: NFT standard (erc721, erc1155, cryptopunks, legacy)
- `nft_quantity`: Number of tokens transferred (always 1 for ERC-721)
- `token_transfer_type`: Specific event type emitted

## Sample Queries

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

**Latest holders for a given ERC-721 collection**
```sql
SELECT 
    to_address,
    contract_address,
    token_id 
FROM <blockchain_name>.nft.ez_nft_transfers
WHERE contract_address = '0xbd3531da5cf5857e7cfaa92426877b022e612cf8'
QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_address, token_id ORDER BY block_number DESC, event_index DESC) =1;
```

{% enddocs %}

{% docs ez_nft_transfers_from_address %}

The address sending/transferring the NFT. Special value of '0x0000000000000000000000000000000000000000' indicates minting event.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_transfers_to_address %}

The address receiving the NFT. Special value of '0x0000000000000000000000000000000000000000' indicates burning event.

Example: '0x1234567890123456789012345678901234567890'

{% enddocs %}

{% docs ez_nft_transfers_token_id %}

The unique identifier for a specific NFT within a collection. String format to handle various token_id formats.

Example: '1234'

{% enddocs %}

{% docs ez_nft_transfers_intra_event_index %}

Position within a batch transfer event, primarily for ERC-1155. Always starts with 1 for single transfers.

Example: 1

{% enddocs %}

{% docs ez_nft_transfers_nft_quantity %}

The number of NFTs transferred for this specific token_id. Always 1 for ERC-721, can be more for ERC-1155.

Example: 1

{% enddocs %}

{% docs ez_nft_transfers_token_transfer_type %}

The specific event type emitted by the contract. Values include 'erc721_Transfer', 'erc1155_TransferSingle', 'erc1155_TransferBatch', etc.

Example: 'erc721_Transfer'

{% enddocs %}

{% docs ez_nft_transfers_is_mint %}

Boolean flag indicating if this transfer is a minting event (from address is 0x0).

Example: true

{% enddocs %}

{% docs ez_nft_transfers_contract_address %}

The address of the contract that emitted the NFT transfer event.

Example: '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'

{% enddocs %}

{% docs ez_nft_transfers_name %}

The name of the NFT collection. For Ethereum only, join with nft.dim_nft_collection_metadata for token-level details.

Example: 'Bored Ape Yacht Club'

{% enddocs %}

{% docs ez_nft_transfers_token_standard %}

The standard of the NFT. Values include 'erc721', 'erc1155', 'cryptopunks', and 'legacy'.

Example: 'erc721'

{% enddocs %}