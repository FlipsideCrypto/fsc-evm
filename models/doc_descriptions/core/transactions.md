{% docs evm_tx_table_doc %}

This table contains transaction level data for this EVM blockchain. Each transaction will have a unique transaction hash, along with transaction fees and a value transferred in the native asset when applicable. Transactions may be native asset transfers or interactions with contract addresses. For more information, please see [The Ethereum Organization - Transactions](https://ethereum.org/en/developers/docs/transactions/)

Below are the specific native tokens that correspond to each EVM chain:

| Status     | Description |
|------------|-------------|
| ETHEREUM   | ETH         |
| BINANCE    | BNB         |
| POLYGON    | POL         |
| AVALANCHE  | AVAX        |
| ARBITRUM   | ETH         |
| OPTIMISM   | ETH         |
| GNOSIS     | xDAI        |
| KAIA       | KLAY        |
| SEI        | SEI         |
| CORE       | CORE        |

{% enddocs %}


{% docs evm_cumulative_gas_used %}

The total amount of gas used when this transaction was executed in the block. 

{% enddocs %}


{% docs evm_tx_block_hash %}

Block hash is a unique 66-character identifier that is generated when a block is produced. 

{% enddocs %}


{% docs evm_tx_fee %}

Amount paid to validate the transaction in the native asset. 

{% enddocs %}


{% docs evm_tx_gas_limit %}

Maximum amount of gas allocated for the transaction. 

{% enddocs %}


{% docs evm_tx_gas_price %}

Cost per unit of gas in Gwei. 

{% enddocs %}


{% docs evm_tx_gas_used %}

Gas used by the transaction.

{% enddocs %}


{% docs evm_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. 

{% enddocs %}


{% docs evm_tx_input_data %}

This column contains additional data for this transaction, and is commonly used as part of a contract interaction or as a message to the recipient.  

{% enddocs %}


{% docs evm_tx_json %}

This JSON column contains the transaction details, including event logs. 

{% enddocs %}


{% docs evm_tx_nonce %}

The number of transactions sent from a given address. 

{% enddocs %}


{% docs evm_tx_origin_sig %}

The function signature of the call that triggered this transaction. 

{% enddocs %}

{% docs evm_origin_sig %}

The function signature of the contract call that triggered this transaction.

{% enddocs %}


{% docs evm_tx_position %}

The position of the transaction within the block. 

{% enddocs %}


{% docs evm_tx_status %}

Status of the transaction. 

{% enddocs %}


{% docs evm_value %}

The value transacted in the native asset. 

{% enddocs %}


{% docs evm_effective_gas_price %}

The total base charge plus tip paid for each unit of gas, in Gwei.

{% enddocs %}

{% docs evm_max_fee_per_gas %}

The maximum fee per gas of the transaction, in Gwei.

{% enddocs %}


{% docs evm_max_priority_fee_per_gas %}

The maximum priority fee per gas of the transaction, in Gwei.

{% enddocs %}


{% docs evm_r %}

The r value of the transaction signature.

{% enddocs %}


{% docs evm_s %}

The s value of the transaction signature.

{% enddocs %}


{% docs evm_v %}

The v value of the transaction signature.

{% enddocs %}

{% docs evm_tx_succeeded %}

Whether the transaction was successful, returned as a boolean.

{% enddocs %}

{% docs evm_tx_fee_precise %}

The precise amount of the transaction fee. This is returned as a string to avoid precision loss. 

{% enddocs %}

{% docs evm_tx_type %}

The type of transaction. 

{% enddocs %}

{% docs evm_mint %}

The minting event associated with the transaction

{% enddocs %}

{% docs evm_source_hash %}

The hash of the source transaction that created this transaction

{% enddocs %}

{% docs evm_eth_value %}

The eth value for the transaction

{% enddocs %}

{% docs evm_chain_id %}

The unique identifier for the chain the transaction was executed on.

{% enddocs %}

{% docs evm_l1_fee_precise_raw %}

The raw l1 fee for the transaction, in Gwei.

{% enddocs %}

{% docs evm_l1_fee_precise %}

The precise l1 fee for the transaction, in Gwei.

{% enddocs %}

{% docs evm_y_parity %}

The y parity for the transaction.

{% enddocs %}

{% docs evm_access_list %}

The access list for the transaction.

{% enddocs %}

{% docs evm_token_ratio %}

Represents the price ratio of ETH and MNT, stored as a protocol-level parameter to help adjust native token fee mechanisms in the network.

{% enddocs %}

{% docs evm_l1_base_fee_scalar %}

The scalar l1 base fee for the transaction.

{% enddocs %}

{% docs evm_l1_blob_base_fee %}

The blob base fee for the transaction.

{% enddocs %}

{% docs evm_l1_blob_base_fee_scalar %}

The scalar blob base fee for the transaction.

{% enddocs %}

{% docs evm_authorization_list %}

Array of authorization entries (EIP-7702) containing information about contracts approved to act on behalf of the EOA.

{% enddocs %}

{% docs evm_operator_fee_scalar %}

Multiplier used in OP Stack chains to calculate operator fees.

{% enddocs %}

{% docs evm_operator_fee_constant %}

Fixed fee amount used in OP Stack chains to calculate operator fees.

{% enddocs %}

{% docs evm_timeboosted %}

Whether the transaction was time boosted (Arbitrum specific).

{% enddocs %}

{% docs evm_blob_versioned_hashes %}

Versioned hashes that uniquely identify the blob data associated with a transaction, used to commit to the blob contents without storing the full data on-chain.

{% enddocs %}

{% docs evm_max_fee_per_blob_gas %}

The maximum fee per unit of blob gas that the transaction sender is willing to pay.

{% enddocs %}

{% docs evm_blob_gas_price %}

The actual price per unit of blob gas paid for the transaction, determined by the blob base fee mechanism that adjusts dynamically based on blob demand.

{% enddocs %}