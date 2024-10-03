{% docs evm_nft_events_table_doc %}

This table contains NFT sales on this EVM blockchain. More NFT marketplaces will be added over time. 

{% enddocs %}

{% docs evm_nft_mint_table_doc %}

This table contains NFT mint events, defined as NFT transfers from a burn address to an address, on this EVM blockchain.

{% enddocs %}

{% docs evm_nft_transfer_table_doc %}

This table contains NFT transfer events on this EVM blockchain.

{% enddocs %}

{% docs evm_nft_aggregator_name %}

The name of the aggregator platform where the sale took place. If the sale did not take place via an aggregator platform, then the value will be null.

{% enddocs %}

{% docs evm_nft_amount %}

The total amount, specified by the mint token address, used as payment to mint the specified number of NFTs corresponding to this token id.

{% enddocs %}

{% docs evm_nft_amount_usd %}

The USD value of 'amount'.

{% enddocs %}

{% docs evm_nft_block_no %}

The block number at which the NFT event occurred.

{% enddocs %}

{% docs evm_nft_blocktime %}

The block timestamp at which the NFT event occurred.

{% enddocs %}

{% docs evm_nft_buyer_address %}

The address of the buyer of the NFT in the transaction. 

{% enddocs %}

{% docs evm_nft_creator_fee %}

The decimal adjusted amount of fees paid to the NFT collection as royalty payments for this NFT event in the transaction's currency. 

{% enddocs %}

{% docs evm_nft_creator_fee_usd %}

The amount of fees paid to the NFT collection as royalty payments for this NFT event in US dollars. 

{% enddocs %}

{% docs evm_nft_currency_address %}

The token contract address for this NFT event. This will be the native asset for native transactions. 

{% enddocs %}

{% docs evm_nft_currency_symbol %}

The token symbol for this NFT event. 

{% enddocs %}

{% docs evm_nft_erc1155_value %}

If the NFT is an ERC-1155 contract, this field may be one or greater, representing the number of tokens. If it is not an ERC-1155 token, this value will be null.

{% enddocs %}

{% docs evm_nft_event_index %}

The event number within a transaction.

{% enddocs %}

{% docs evm_nft_event_type %}

The type of NFT event in this transaction, either `sale`, `bid_won`, `redeem`, or `mint`.

{% enddocs %}

{% docs evm_nft_from_address %}

The sending address of the NFT in the transaction. 

{% enddocs %}

{% docs evm_nft_intra_event_index %}

The order of events within a single event index. This is primarily used for ERC1155 NFT batch transfer events. 

{% enddocs %}

{% docs evm_nft_metadata %}

The token metadata for this NFT. This may be blank for many NFTs. We are working to expand this field. 

{% enddocs %}

{% docs evm_nft_mint_count %}

The number of NFTs minted in this event.

{% enddocs %}

{% docs evm_nft_mint_price %}

The price paid in the native asset to mint the NFT(s).

{% enddocs %}

{% docs evm_nft_mint_price_usd %}

The price paid in US dollars to mint the NFT(s).

{% enddocs %}

{% docs evm_nft_mints_symbol %}

The symbol of the token supplied to mint the NFT, if applicable. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_address %}

The contract address of the token supplied to mint the NFT, if applicable. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_price %}

The decimal adjusted amount of tokens supplied within the same transaction to mint the NFT. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_price_usd %}

The amount of tokens supplied in US dollars within the same transaction to mint the NFT. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_nft_address %}

The contract address of the NFT.

{% enddocs %}

{% docs evm_nft_origin_from %}

The from address of this transaction. In most cases, this is the NFT buyer. However, for some more complex transactions, it may not be the NFT buyer.

{% enddocs %}

{% docs evm_nft_origin_sig %}

The function signature of this transaction.

{% enddocs %}

{% docs evm_nft_origin_to %}

The to address of this transaction. In most cases, this is the exchange contract. However, for some more complex NFT events, such as aggregate buys with tools, this may not be the exchange address. 

{% enddocs %}

{% docs evm_nft_platform_address %}

The address of the exchange used for the transaction.

{% enddocs %}

{% docs evm_nft_platform_exchange_version %}

The version of the exchange contract used for the transaction.

{% enddocs %}

{% docs evm_nft_platform_fee %}

The decimal adjusted amount of fees paid to the platform for this NFT event in the transaction's currency. There are cases where there are fees paid to multiple marketplaces. In those cases, the fee in the platform_fee column will only reflect the platform fee related to the platform exchange contract.

{% enddocs %}

{% docs evm_nft_platform_fee_usd %}

The amount of fees paid to the platform for this NFT event in US dollars. There are cases where there are fees paid to multiple marketplaces. In those cases, the fee in the platform_fee column will only reflect the platform fee related to the platform exchange contract.

{% enddocs %}

{% docs evm_nft_platform_name %}

The name of the exchange used for the trade. 

{% enddocs %}

{% docs evm_nft_price %}

The total price of the NFT, in the currency in which the transaction occurred and decimal adjusted where possible. Please note that the price of the NFT, after subtracting total fees, may not represent the net amount paid to the seller in all cases. You may refer to the platform fee description for more info. 

{% enddocs %}

{% docs evm_nft_price_usd %}

The total price of the NFT in US dollars. This will be 0 for tokens without a decimal adjustment or hourly price. Please note that the price of the NFT, after subtracting total fees, may not represent the net amount paid to the seller in all cases. You may refer to the platform fee description for more info. 

{% enddocs %}

{% docs evm_nft_project_name %}

The name of the NFT project. This field, along with metadata, will be filled in over time.

{% enddocs %}

{% docs evm_nft_seller_address %}

The address of the seller of the NFT in the transaction. 

{% enddocs %}

{% docs evm_nft_to_address %}

The receiving address of the NFT in the transaction. 

{% enddocs %}

{% docs evm_nft_tokenid %}

The token ID for this NFT contract. 

{% enddocs %}

{% docs evm_nft_total_fees %}

The total amount of fees paid relating to the NFT purchase in the transaction currency. This includes royalty payments to creators and platform fees. Please note, this does not include the gas fee.

{% enddocs %}

{% docs evm_nft_total_fees_usd %}

The total amount of fees paid relating to the NFT purchase in US dollars. This includes royalty payments to creators and platform fees. Please note, this does not include the gas fee.

{% enddocs %}

{% docs evm_nft_tx_fee %}

The gas fee for this transaction in the native asset. 

{% enddocs %}

{% docs evm_nft_tx_fee_usd %}

The gas fee for this transaction in US dollars. 

{% enddocs %}

{% docs evm_nft_tx_hash %}

The transaction hash for the NFT event. This is not necessarily unique in this table as a transaction may contain multiple NFT events. 

{% enddocs %}

