{% docs evm_tx_table_doc %}

This table contains transaction level data for this EVM blockchain. Each transaction will have a unique transaction hash, along with transaction fees and a value transferred in the native asset when applicable. Transactions may be native asset transfers or interactions with contract addresses. For more information, please see [The Ethereum Organization - Transactions](https://ethereum.org/en/developers/docs/transactions/)

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

The function signature of the contract call. 

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


