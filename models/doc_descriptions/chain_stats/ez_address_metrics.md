{% docs ez_address_metrics_table_doc %}

## What

User-level aggregated metrics for the indicated EVM blockchain including transaction activity, DeFi interactions, staking behavior, NFT activity, and governance participation. Captures comprehensive on-chain behavior patterns for individual addresses across various protocol categories and activity types.

{% enddocs %}

{% docs ez_address_metrics_address %}

Unique address (wallet address) - primary identifier for all aggregated metrics

Example: '0x1234567890abcdef1234567890abcdef12345678'

{% enddocs %}

{% docs ez_address_metrics_n_complex_txn %}

Number of non-native-transfer transactions initiated by the user + number of bridge transactions

{% enddocs %}

{% docs ez_address_metrics_n_contracts %}

The number of different contracts that the user transacts with

{% enddocs %}

{% docs ez_address_metrics_n_days_active %}

Number of days with initiated transactions + CEX withdrawals + inbound bridge transfers

{% enddocs %}

{% docs ez_address_metrics_n_txn %}

Number of transactions + CEX withdrawals + inbound bridge transfers

{% enddocs %}

{% docs ez_address_metrics_n_bridge_in %}

Number of inbound bridge transfers

{% enddocs %}

{% docs ez_address_metrics_n_bridges %}

Total number of bridge transactions (both inbound and outbound) executed by this address

{% enddocs %}

{% docs ez_address_metrics_n_cex_withdrawals %}

Number of withdrawals from a centralized exchange

{% enddocs %}

{% docs ez_address_metrics_net_token_accumulate %}

Number of token transfers received / (number of token transfers received + number of token transfers sent)

{% enddocs %}

{% docs ez_address_metrics_n_other_defi %}

Any non-swap, non-LP transactions with events like borrow, lend, etc.

{% enddocs %}

{% docs ez_address_metrics_n_lp_adds %}

Number of non-swap transfers to a liquidity pool

{% enddocs %}

{% docs ez_address_metrics_n_lp_pools %}

Number of unique liquidity pools this address has provided liquidity to

{% enddocs %}

{% docs ez_address_metrics_n_swap_tx %}

Number of swaps

{% enddocs %}

{% docs ez_address_metrics_n_swaps %}

Total number of individual swap operations (may differ from n_swap_tx if transactions contain multiple swaps)

{% enddocs %}

{% docs ez_address_metrics_n_tokens_traded %}

Number of distinct tokens swapped

{% enddocs %}

{% docs ez_address_metrics_n_nft_collections %}

Number of NFT contracts traded

{% enddocs %}

{% docs ez_address_metrics_n_nft_mints %}

Number of NFTs minted

{% enddocs %}

{% docs ez_address_metrics_n_nft_buys %}

Number of buys of any NFTs

{% enddocs %}

{% docs ez_address_metrics_n_nft_ids %}

Number of distinct NFTs bought or sold

{% enddocs %}

{% docs ez_address_metrics_n_nft_lists %}

Number of NFTs listed

{% enddocs %}

{% docs ez_address_metrics_n_votes %}

Number of staking transactions (liquid stake or delegation)

{% enddocs %}

{% docs ez_address_metrics_n_stake_tx %}

Number of staking transactions (liquid stake or delegation)

{% enddocs %}

{% docs ez_address_metrics_n_restakes %}

Number of restakes

{% enddocs %}

{% docs ez_address_metrics_n_validators %}

Number of validators staked to OR liquid stake providers

{% enddocs %}

{% docs ez_address_metrics_net_stake_accumulate %}

Number of stakes / (number of stakes + number of unstakes)

{% enddocs %}

