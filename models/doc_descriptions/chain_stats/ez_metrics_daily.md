{% docs ez_metrics_daily_table_doc %}

## What

Daily aggregated metrics for the indicated EVM blockchain including transaction activity, fees, DEX volumes, bridge flows, CEX flows, and TVL data. All metrics are calculated at the daily level (YYYY-MM-DD format) and include both total activity and quality user activity (Flipside score >= 4).

{% enddocs %}

{% docs ez_metrics_daily_day_ %}

The date in YYYY-MM-DD format - all stats are aggregated at the daily level

{% enddocs %}

{% docs ez_metrics_daily_active_users_count %}

Number of origin_from_address (Externally Owned Accounts EOAs) that submitted a transaction

{% enddocs %}

{% docs ez_metrics_daily_active_quality_users_count %}

Number of EOAs with a Flipside score of 4 or higher that submitted a transaction

{% enddocs %}

{% docs ez_metrics_daily_transaction_count %}

Number of transactions submitted by any origin_from_address (EOA)

{% enddocs %}

{% docs ez_metrics_daily_quality_transaction_count %}

Number of transactions submitted by EOAs with a Flipside score of 4 or higher

{% enddocs %}

{% docs ez_metrics_daily_total_fees %}

Total transaction fees paid, denominated in Ether (ETH)

{% enddocs %}

{% docs ez_metrics_daily_total_fees_usd %}

USD denominated total transaction fees paid (converting ETH to USD via ETH price)

{% enddocs %}

{% docs ez_metrics_daily_quality_total_fees %}

Total transaction fees paid by EOAs with a Flipside score of 4 or higher, denominated in Ether (ETH)

{% enddocs %}

{% docs ez_metrics_daily_quality_total_fees_usd %}

USD denominated total transaction fees paid by EOAs with a Flipside score of 4 or higher (converting ETH to USD via ETH price)

{% enddocs %}

{% docs ez_metrics_daily_stablecoin_transfer_volume_usd %}

Value of all stablecoin transfers for any reason (USD)

{% enddocs %}

{% docs ez_metrics_daily_in_unit_total_transfer_volume %}

Value of all token transfers of any token for any reason, denominated in ETH to reduce price effects (token price changes can be different than fundamental activity changes)

{% enddocs %}

{% docs ez_metrics_daily_total_transfer_volume_usd %}

Value of all token transfers for any reason (direct, swap, liquidity deposit, staking, etc.), denominated in USD accepting price effects

{% enddocs %}

{% docs ez_metrics_daily_in_unit_quality_total_transfer_volume %}

Value of all token transfers FROM EOAs with a Flipside score of 4 or higher for any reason, denominated in ETH

{% enddocs %}

{% docs ez_metrics_daily_quality_total_transfer_volume_usd %}

Value of all token transfers FROM EOAs with a Flipside score of 4 or higher for any reason, denominated in USD

{% enddocs %}

{% docs ez_metrics_daily_cex_withdrawal_volume_usd %}

Value of central exchange token withdrawals (USD)

{% enddocs %}

{% docs ez_metrics_daily_cex_withdrawal_tx_count %}

Number of central exchange token withdrawal transactions

{% enddocs %}

{% docs ez_metrics_daily_cex_unique_withdrawing_addresses %}

Number of unique addresses that withdrew from a central exchange

{% enddocs %}

{% docs ez_metrics_daily_cex_deposit_volume_usd %}

Value of central exchange token deposits (USD)

{% enddocs %}

{% docs ez_metrics_daily_cex_deposit_tx_count %}

Number of central exchange token deposit transactions

{% enddocs %}

{% docs ez_metrics_daily_cex_unique_depositing_addresses %}

Number of unique addresses that deposited to a central exchange

{% enddocs %}

{% docs ez_metrics_daily_cex_net_flow_usd %}

Net CEX flow (withdrawal_volume_usd - deposit_volume_usd). Note that if activity and price are correlated, this value can be negative even if more tokens are withdrawn than deposited

{% enddocs %}

{% docs ez_metrics_daily_chain_gross_dex_volume_usd %}

USD value of token sell volume on Decentralized Exchanges (DEXs) protocols

{% enddocs %}

{% docs ez_metrics_daily_chain_swap_count %}

Number of swap transactions on Decentralized Exchanges (DEXs) protocols

{% enddocs %}

{% docs ez_metrics_daily_chain_swapper_count %}

Number of unique EOAs that have submitted a swap transaction on Decentralized Exchanges (DEXs) protocols

{% enddocs %}

{% docs ez_metrics_daily_tvl_usd %}

Total Value Locked - USD value of tokens locked in smart contracts. INCLUDES borrowed tokens, liquid staking, and staking

{% enddocs %}

{% docs ez_metrics_daily_in_unit_tvl %}

Total Value Locked in ETH denominated terms to reduce price effects

{% enddocs %}

{% docs ez_metrics_daily_bridge_inbound_volume_usd %}

Value of bridge inflows, denominated in USD

{% enddocs %}

{% docs ez_metrics_daily_bridge_inbound_addresses %}

Number of unique addresses receiving a bridge inflow

{% enddocs %}

{% docs ez_metrics_daily_bridge_inbound_tx_count %}

Number of bridge inflow transactions

{% enddocs %}

{% docs ez_metrics_daily_bridge_outbound_volume_usd %}

Value of bridge outflows, denominated in USD

{% enddocs %}

{% docs ez_metrics_daily_bridge_outbound_addresses %}

Number of unique addresses sending a bridge outflow

{% enddocs %}

{% docs ez_metrics_daily_bridge_outbound_tx_count %}

Number of bridge outflow transactions

{% enddocs %}

{% docs ez_metrics_daily_bridge_gross_volume_usd %}

Value of bridge outflows, denominated in USD

{% enddocs %}

{% docs ez_metrics_daily_bridge_net_inbound_usd %}

Value of bridge inflows minus outflows, denominated in USD. Note that if activity and price are correlated, this value can be negative even if more tokens are brought onto chain than off

{% enddocs %}

