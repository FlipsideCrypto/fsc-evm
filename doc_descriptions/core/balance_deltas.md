{% docs evm_balance_deltas_block_number %}

Block at which the balance was read (when the transfer occurred).

{% enddocs %}


{% docs evm_balance_deltas_block_timestamp %}

Block timestamp at which the balance was read (when the transfer occurred).

{% enddocs %}


{% docs evm_balance_deltas_user_address %}

The wallet address holding the tokens / native asset.

{% enddocs %}


{% docs evm_balance_deltas_contract_address %}

The contract address of the token (null for native asset).

{% enddocs %}


{% docs evm_balance_deltas_prev_bal_unadj %}

The token or native asset balance from the previously recorded record for this wallet / token, without a decimal adjustment.

{% enddocs %}


{% docs evm_balance_deltas_prev_bal %}

The decimal adjusted token or native asset balance from the previously recorded record for this wallet and token.

{% enddocs %}


{% docs evm_balance_deltas_prev_bal_usd %}

Previously recorded balance in USD - this will be null for tokens without a decimal adjustment. Please note, the USD value is calculated at this block.

{% enddocs %}


{% docs evm_balance_deltas_current_bal_unadj %}

The token or native asset balance at the current block number, without a decimal adjustment.

{% enddocs %}


{% docs evm_balance_deltas_ current_bal %}

The decimal adjusted token or native asset balance at the current block number.

{% enddocs %}


{% docs evm_balance_deltas_current_bal_usd %}

The current balance in USD - this will be null for tokens without a decimal adjustment. Please note, the USD value is calculated at this block.

{% enddocs %}


{% docs evm_bal_delta_unadj %}

The non-decimal adjusted balance change.

{% enddocs %}


{% docs evm_bal_delta %}

The decimal adjusted balance change.

{% enddocs %}


{% docs evm_bal_delta_usd %}

The balance change in USD, will be null for tokens without a decimal.

{% enddocs %}


{% docs evm_symbol %}

The symbol of the token contract, or native asset.

{% enddocs %}


{% docs evm_token_name %}

The name of the token contract, or native asset.

{% enddocs %}


{% docs evm_decimals %}

The decimals for the token contract.

{% enddocs %}


{% docs evm_has_decimal %}

Whether the token has a decimal or not, either TRUE or FALSE.

{% enddocs %}


{% docs evm_has_price %}

Whether the token has an hourly price or not, either TRUE or FALSE.

{% enddocs %}


