{% docs evm_all_ctoken %}

Address of the Compound token.

{% enddocs %}


{% docs evm_all_ctoken_symbol %}

Symbol of the Compound token.

{% enddocs %}


{% docs evm_borrows_borrower %}

Address that initiated a borrow event.

{% enddocs %}


{% docs evm_borrows_contract_address %}

Address of borrowed token.

{% enddocs %}


{% docs evm_borrows_contract_symbol %}

Symbol of borrowed token.

{% enddocs %}


{% docs evm_borrows_loan_amount %}

Native amount of borrow (decimal adjusted).

{% enddocs %}


{% docs evm_borrows_loan_amount_usd %}

The equivalent borrow amount in USD. Note this is computed by taking the average hourly price around the time of the tx event.

{% enddocs %}


{% docs evm_borrows_table_doc %}

Borrows exist within the compound schema, as compound.ez_borrows

{% enddocs %}


{% docs evm_comp_asset_table_doc %}

Contains details, such as decimals, name, and other relevant contract metadata for Compound assets.

{% enddocs %}


{% docs evm_comp_ctoken_decimals %}

The cToken decimals.

{% enddocs %}


{% docs evm_comp_ctoken_name %}

The cToken name.

{% enddocs %}


{% docs evm_comp_ctoken_symbol %}

The cToken symbol.

{% enddocs %}


{% docs evm_comp_redeemer %}

Address of the redeemer. 

{% enddocs %}


{% docs evm_comp_underlying %}

The underlying asset for the cToken.

{% enddocs %}


{% docs evm_comp_underlying_decimals %}

The underlying asset decimals.

{% enddocs %}


{% docs evm_comp_underlying_name %}

The underlying asset name.

{% enddocs %}


{% docs evm_comp_underlying_symbol %}

The underlying asset symbol.

{% enddocs %}


{% docs evm_deposits_issued_ctokens %}

Amount of cToken issued for providing liquidity.

{% enddocs %}


{% docs evm_deposits_supplied_base_asset %}

Native amount provided as liquidity (decimal adjusted).

{% enddocs %}


{% docs evm_deposits_supplied_base_asset_usd %}

The equivalent liquidity amount in USD. Note this is computed by taking the average hourly price around the time of the tx event.

{% enddocs %}


{% docs evm_deposits_supplied_contract_addr %}

Address of token provided liquidity for.

{% enddocs %}


{% docs evm_deposits_supplied_symbol %}

Symbol of token provided liquidity for.

{% enddocs %}


{% docs evm_deposits_supplier %}

Address of liquidity provider.

{% enddocs %}


{% docs evm_deposits_table_doc %}

Deposits exist within the compound schema, as compound.ez_deposits

{% enddocs %}


{% docs evm_liquidations_ctokens_seized %}

cToken collateral held by the insolvent borrower that is taken by the liquidator.

{% enddocs %}


{% docs evm_liquidations_liquidation_amount %}

Native amount liquidated (decimal adjusted).

{% enddocs %}


{% docs evm_liquidations_liquidation_contract_address %}

Address of liquidated token.

{% enddocs %}


{% docs evm_liquidations_liquidation_contract_symbol %}

Symbol of liquidated token.

{% enddocs %}


{% docs evm_liquidations_liquidations_amount_usd %}

The equivalent liquidated amount in USD. Note this is computed by taking the average hourly price around the time of the tx event.

{% enddocs %}


{% docs evm_liquidations_liquidator %}

Address that got liquidated.

{% enddocs %}


{% docs evm_liquidations_table_doc %}

Liquidations exist within the compound schema, as compound.ez_liquidations

{% enddocs %}


{% docs evm_market_stats_underlying_contract %}

Address of the underlying token the market serves (i.e. USDC).

{% enddocs %}


{% docs evm_market_stats_block_hour %}

Market stats are aggregated by hour in UTC. date_trunc(‘hour’,block_timestamp) for joins on other tables.

{% enddocs %}


{% docs evm_market_stats_borrow_apy %}

The borrower’s APY in terms of the underlying asset. It depends on the exchange rate between the cToken/underlying token (cUSDC/USDC). This is interest paid by the borrower on their loan.

{% enddocs %}


{% docs evm_market_stats_borrows_token_amount %}

Amount borrowed from the market.

{% enddocs %}


{% docs evm_market_stats_borrows_usd %}

Borrows converted to USD values as of the hour recorded.

{% enddocs %}


{% docs evm_market_stats_comp_apy_borrow %}

The APY one can expect based on COMP governance tokens distributed (which in turn can be staked elsewhere, or used in voting).

{% enddocs %}


{% docs evm_market_stats_comp_apy_supply %}

The APY one can expect based on COMP governance tokens distributed (which in turn can be staked elsewhere, or used in voting).

{% enddocs %}


{% docs evm_market_stats_comp_price %}

The price of the COMP governance token.

{% enddocs %}


{% docs evm_market_stats_comp_speed %}

COMP is a governance token distributed equally to both suppliers and borrowers (the idea being the users of the protocol are also the ones who should be able to vote on governance actions). Comp speed controls the rate at which comp is distributed to users of the market, per block.

{% enddocs %}


{% docs evm_market_stats_comp_speed_usd %}

Comp distributed to markets converted to USD.

{% enddocs %}


{% docs evm_market_stats_contract_name %}

market/cToken name

{% enddocs %}


{% docs evm_market_stats_ctoken_price %}

Price of the cToken (i.e. cUSDC).

{% enddocs %}


{% docs evm_market_stats_reserves_token_amount %}

Reserves are amounts set aside by the market that can be used/affected by governance actions through proposals voted on by COMP holders.

{% enddocs %}


{% docs evm_market_stats_reserves_usd %}

Reserves converted to USD values as of the hour recorded.

{% enddocs %}


{% docs evm_market_stats_supply_apy %}

The supplier’s APY in terms of the underlying asset. It depends on the exchange rate between the cToken/underlying token (cUSDC/USDC). This is interest paid to the supplier for their stake.

{% enddocs %}


{% docs evm_market_stats_supply_token_amount %}

Amount (in terms of the cToken) supplied to the market through suppliers.

{% enddocs %}


{% docs evm_market_stats_supply_usd %}

Supply converted to USD values as of the hour recorded.

{% enddocs %}


{% docs evm_market_stats_table_doc %}

Market Stats exist within the compound schema, as compound.ez_market_stats

{% enddocs %}


{% docs evm_market_stats_token_price %}

Price of the underlying token (i.e. USDC).

{% enddocs %}


{% docs evm_market_stats_underlying_symbol %}

Symbol of the underlying token the market serves.

{% enddocs %}


{% docs evm_redemptions_received_amount %}

Native amount provided as liquidity (decimal adjusted).

{% enddocs %}


{% docs evm_redemptions_received_amount_usd %}

The equivalent liquidity amount in USD. Note this is computed by taking the average hourly price around the time of the tx event.

{% enddocs %}


{% docs evm_redemptions_received_contract_address %}

Address of token refunded as part of the redemption.

{% enddocs %}


{% docs evm_redemptions_received_contract_symbol %}

Symbol of token refunded as part of the redemption.

{% enddocs %}


{% docs evm_redemptions_redeemed_ctoken %}

cToken deposited to redeem 

{% enddocs %}


{% docs evm_redemptions_table_doc %}

Redemptions exist within the compound schema, as compound.ez_redemptions

{% enddocs %}


{% docs evm_repayments_payer %}

Address of user that paid out the loan

{% enddocs %}


{% docs evm_repayments_repay_contract_address %}

Address of token refunded as part of the redemption

{% enddocs %}


{% docs evm_repayments_repay_contract_symbol %}

Symbol of token refunded as part of the redemption

{% enddocs %}


{% docs evm_repayments_repayed_amount %}

Native amount repaid on loan  (decimal adjusted)

{% enddocs %}


{% docs evm_repayments_repayed_amount_usd %}

The equivalent repaid amount in USD. Note this is computed by taking the average hourly price around the time of the tx event

{% enddocs %}


{% docs evm_repayments_table_doc %}

Repayments exist within the compound schema, as compound.ez_repayments

{% enddocs %}


