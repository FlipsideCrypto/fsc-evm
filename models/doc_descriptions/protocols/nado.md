{% docs nado_dim_products %}

All available Nado products, these are automatically added as they are released on chain.


{% enddocs %}

{% docs nado_money_markets %}

Nado integrates a decentralized money market directly into its DEX, enabling borrowing and lending of crypto assets using overcollateralized lending rules. Interest rates are dynamically adjusted based on supply and demand, incentivizing liquidity provision and balancing borrowing costs. The money market operates on-chain (e.g., on Arbitrum) and is managed through Nado’s risk engine and clearinghouse, offering users automated borrowing via portfolio margin and passive yield opportunities on idle assets. This table tracks the money market products available on Nado on an hourly basis.


{% enddocs %}

{% docs nado_liquidations %}

All Nado liquidations. Once an account’s maintenance margin reaches $0, the account is eligible for liquidation. Liquidation events happen one by one, with the riskiest positions being liquidated first. Liquidations are based on the oracle price.


{% enddocs %}

{% docs nado_perp_trades %}

Nado perpetuals are derivative contracts on an underlying spot asset. On Nado, all perpetual contracts trade against USDC.

**Important: Volume Calculation**
Each trade match emits **two** FillOrder events on-chain: one for the maker and one for the taker (each with their own order digest). To calculate accurate volume or trade counts that match Nado's official API metrics, filter to `is_taker = TRUE` to avoid double-counting.

{% enddocs %}

{% docs nado_spot_trades %}

Nado's spot markets allow you to buy or sell listed crypto assets paired with USD-denominated stablecoins.

**Important: Volume Calculation**
Each trade match emits **two** FillOrder events on-chain: one for the maker and one for the taker (each with their own order digest). To calculate accurate volume or trade counts that match Nado's official API metrics, filter to `is_taker = TRUE` to avoid double-counting.

{% enddocs %}

{% docs nado_clearing_house_events %}

Nado’s on-chain clearinghouse operates as the hub combining perpetual and spot markets, collateral, and risk calculations into a single integrated system. The events in this table track when a wallet either deposits or withdraws from the clearinghouse contract.

{% enddocs %}

{% docs nado_account_stats %}

Subaccount level table showing aggregated total activity across the Nado exchange.

{% enddocs %}

{% docs nado_market_stats %}

Orderbook level market stats based on a combination of on-chain data and data from Nado's ticker V2 API which includes 24-hour pricing and volume information on each market pair available on Nado.

{% enddocs %}

{% docs nado_market_depth %}

Liquidity data taken from Nado's Orderbook API, showing amount of liquidity at each price level.

{% enddocs %}

{% docs nado_staking  %}

All staking actions taken with the VRTX staking contract.

{% enddocs %}

{% docs nado_symbol %}

The specific Nado product symbol, if it is a futures product it will have a -PERP suffix.

{% enddocs %}

{% docs nado_deposit_apr %}

The recorded deposit APR for the money market product in that hour.

{% enddocs %}

{% docs nado_borrow_apr %}

The recorded borrow APR for the money market product in that hour.

{% enddocs %}

{% docs nado_tvl %}

The sum total value locked for the money market product in that hour.

{% enddocs %}

{% docs nado_digest %}

The identifier for a specific trade, this can be split across two or more base deltas in order to fill the entire amount of the trade.

{% enddocs %}

{% docs nado_trader %}

The wallet address of the trader, there can be multiple subaccounts associated with a trader.

{% enddocs %}

{% docs nado_subaccount %}

Independent Nado account of trader with its own margin, balance, positions, and trades. Any wallet can open an arbitrary number of these. Risk is not carried over from subaccount to subaccount.

{% enddocs %}

{% docs nado_trade_type %}

They type of trade taken, long/short for perps or buy/sell for spot.

{% enddocs %}

{% docs nado_expiration %}

Time after which the order should automatically be cancelled, as a timestamp in seconds after the unix epoch, converted to datetime.

{% enddocs %}

{% docs nado_order_type %}

Decode from raw expiration number to binary then converted back to int from the most significant two bits: 
0 ⇒ Default order, where it will attempt to take from the book and then become a resting limit order if there is quantity remaining
1 ⇒ Immediate-or-cancel order, which is the same as a default order except it doesn’t become a resting limit order
2 ⇒ Fill-or-kill order, which is the same as an IOC order except either the entire order has to be filled or none of it.
3 ⇒ Post-only order, where the order is not allowed to take from the book. An error is returned if the order would cross the bid ask spread.

{% enddocs %}

{% docs nado_market_reduce_flag %}

A reduce-only is an order that will either close or reduce your position. The reduce-only flag can only be set on IOC or FOK order types. Send a reduce-only order by setting the 3rd most significant bit on the expiration field.

{% enddocs %}

{% docs nado_nonce %}

Number used to differentiate between the same order multiple times, and a user trying to place an order with the same parameters twice. Represented as a string.

{% enddocs %}

{% docs nado_is_taker %}

Boolean representing if the trader was the taker or maker. Each trade match emits two FillOrder events (one for each side), so when calculating volume or trade counts, filter to `is_taker = TRUE` to avoid double-counting.

{% enddocs %}

{% docs nado_price_amount_unadj %}

The price amount that the trade was executed at.

{% enddocs %}

{% docs nado_price_amount %}

The price amount that the trade was executed at, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs nado_amount_unadj %}

The total size of the trade in units of the asset being traded.

{% enddocs %}

{% docs nado_amount %}

The total size of the trade in units of the asset being traded across one digest, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs nado_amount_usd %}

The size of the trade in USD. Base Delta multiplied by the price amount.

{% enddocs %}

{% docs nado_fee_amount_unadj %}

The fees on the trade.

{% enddocs %}

{% docs nado_fee_amount %}

The fees on the trade, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs nado_base_delta_amount_unadj %}

Represents the net change in the total quantity of orders at a particular price level, the sum of these across the same digest is equal to the amount. This is the first currency listed in the pair and acts as the reference point for the exchange rate, in this case the crypto asset trading against USDC.

{% enddocs %}

{% docs nado_base_delta_amount %}

Represents the net change in the total quantity of orders at a particular price level, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. The sum of these across the same digest is equal to the amount. This is the first currency listed in the pair and acts as the reference point for the exchange rate, in this case the crypto asset trading against USDC.

{% enddocs %}

{% docs nado_quote_delta_amount_unadj %}

A positive value is an increase in spread and a negative value is a decrease in spread. Quote is currency used to express the value of the base currency. It's often the more well-known or stable currency in the pair. In this case, USDC.

{% enddocs %}

{% docs nado_quote_delta_amount %}

The net change in the best bid and best ask prices in the order book, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. A positive value is an increase in spread and a negative value is a decrease in spread. Quote is currency used to express the value of the base currency. It's often the more well-known or stable currency in the pair. In this case, USDC.

{% enddocs %}

{% docs nado_mode %}

The type of liquidation, 0 being a LP position, 1 being a balance - ie a Borrow, and 2 being a perp position.

Only available in Nado V1, live until March 8th 2024.

{% enddocs %}

{% docs nado_health_group %}

The spot / perp product pair of health group i where health_groups[i][0] is the spot product_id and health_groups[i][1] is the perp product_id. Additionally, it is possible for a health group to only have either a spot or perp product, in which case, the product that doesn’t exist is set to 0.

{% enddocs %}

{% docs nado_health_group_symbol %}

The token symbol represented by the specific health group. For example WBTC and BTC-PERP is BTC.

{% enddocs %}

{% docs nado_amount_quote_unadj %}

To liquidate a position, there must be a payment (transfer) between the liquidator and the position holder. This done in the quote currency, USDC. Payments are signed as positive, meaning you received the USDC, or negative, meaning you paid. For perpetual liquidations, users should expect to see a (+) USDC payment. They will see a (-) USDC payment for borrowers since they need to pay the user for buying their borrow.

{% enddocs %}

{% docs nado_amount_quote %}

To liquidate a position, there must be a payment (transfer) between the liquidator and the position holder. This done in the quote currency, USDC. Payments are signed as positive, meaning you received the USDC, or negative, meaning you paid. For perpetual liquidations, users should expect to see a (+) USDC payment. They will see a (-) USDC payment for borrowers since they need to pay the user for buying their borrow. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract.

{% enddocs %}

{% docs nado_insurance_cover_unadj %}

USDC from the insurance fund pulled into the insolvent account and used to pay liquidators to take on the underwater positions.

Only available in Nado V1, live until March 8th 2024.

{% enddocs %}

{% docs nado_insurance_cover %}

USDC from the insurance fund pulled into the insolvent account and used to pay liquidators to take on the underwater positions, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract.

Only available in Nado V1, live until March 8th 2024.

{% enddocs %}

{% docs nado_book_address %}

The contract address associated with each product, this is where all fill orders are published to the chain.

{% enddocs %}

{% docs nado_product_type %}

The type of product, either spot or perpetual futures.

{% enddocs %}

{% docs nado_product_id %}

The unique id of each product. Evens are perp products and odds are spot products.

{% enddocs %}

{% docs nado_ticker_id %}

Identifier of a ticker with delimiter to separate base/target.

{% enddocs %}

{% docs nado_name %}

The name of the product

{% enddocs %}

{% docs nado_version %}

The version of Nado with version 2 on or after March 8th 2024.

{% enddocs %}

{% docs nado_token_address %}

The underlying asset token address deposited or withdrawn from the clearinghouse contract.

{% enddocs %}

{% docs nado_amount_usd_ch %}

The size of deposit or withdraw in USD.

{% enddocs %}

{% docs nado_product_id_liq %}

The product to liquidate as well as the liquidation mode:
Perp Liquidation: Any valid perp product_id with is_encode_spread set to false. 
Spot Liquidation: Any valid spot product_id with is_encode_spread set to false. 
Spread Liquidation: If there are perp and spot positions in different directions, liquidate both at the same time. is_encode_spread must be set to true.

If it is a spread liquidation this column will show the perp product_id, for both ids refer to the spread_product_ids array.

Only available in V2 Nado liquidations, which went live March 8th 2024. 

{% enddocs %}

{% docs nado_is_encode_spread %}

Indicates whether product_id encodes both a spot and perp product_id for spread_liquidation.

Only available in V2 Nado liquidations, which went live March 8th 2024. 

{% enddocs %}

{% docs nado_decoded_spread_product_ids %}

Array of product_ids that have been decoded from binary. Only available when is_encode_spread is true and the liquidation occurs on V2 Nado, which went live March 8th 2024. 

{% enddocs %}

{% docs nado_first_trade_timestamp %}

The block timestamp of this subaccounts first trade.

{% enddocs %}

{% docs nado_last_trade_timestamp %}

The block timestamp of this subaccounts most recent trade.

{% enddocs %}

{% docs nado_account_age %}

The age of the account in days.

{% enddocs %}

{% docs nado_trade_count %}

The total amount of trades executed by the account

{% enddocs %}

{% docs nado_trade_count_rank %}

The rank against all accounts based on trade count volume.

{% enddocs %}

{% docs nado_trade_count_24h %}

The total amount of trades made in the last 24 hours.

{% enddocs %}

{% docs nado_trade_count_rank_24h %}

The rank against all accounts based on trade count volume in the last 24 hours.

{% enddocs %}

{% docs nado_perp_trade_count %}

The total amount of perpetual trades executed by the account

{% enddocs %}

{% docs nado_spot_trade_count %}

The total amount of spot trades executed by the account

{% enddocs %}

{% docs nado_long_count %}

The total amount of buys/longs on the account.

{% enddocs %}

{% docs nado_short_count %}

The total amount of sell/shorts on the account.

{% enddocs %}

{% docs nado_total_usd_volume %}

The total USD denominated volume of the account.

{% enddocs %}

{% docs nado_total_usd_volume_24h %}

The total USD denominated volume of the account in the last 24 hours.

{% enddocs %}

{% docs nado_total_usd_volume_rank_24h %}

The rank against all accounts based on the total USD denominated volume of the account in the last 24 hours.

{% enddocs %}

{% docs nado_total_usd_volume_rank %}

The rank against all accounts based on total usd volume on the account.

{% enddocs %}

{% docs nado_avg_usd_trade_size %}

The average trade size in USD.

{% enddocs %}

{% docs nado_total_fee_amount %}

The total amount of trading fees paid by the account.

{% enddocs %}

{% docs nado_total_base_delta_amount %}

The total base delta amount of the account.

{% enddocs %}

{% docs nado_total_quote_delta_amount %}

The total quote delta amount of the account.

{% enddocs %}

{% docs nado_total_liquidation_amount %}

The total liquidation amount of the account.

{% enddocs %}

{% docs nado_total_liquidation_count %}

The total count of liquidation accounts on the account.

{% enddocs %}

{% docs nado_orderbook_side %}

Designates the bid or ask side of the orderbook price.

{% enddocs %}

{% docs nado_orderbook_volume %}

The quantity for each bid/ask order at the given price level.

{% enddocs %}

{% docs nado_orderbook_price %}

The price level for each bid/ask order.

{% enddocs %}

{% docs nado_orderbook_round_price_0_01 %}

The price level for each bid/ask order, rounded to nearest cent. 

{% enddocs %}

{% docs nado_orderbook_round_price_0_1 %}

The price level for each bid/ask order, rounded to nearest ten cents. 

{% enddocs %}

{% docs nado_orderbook_round_price_1 %}

The price level for each bid/ask order, rounded to nearest dollar. 

{% enddocs %}

{% docs nado_orderbook_round_price_10 %}

The price level for each bid/ask order, rounded to nearest 10 dollars. 

{% enddocs %}

{% docs nado_orderbook_round_price_100 %}

The price level for each bid/ask order, rounded to nearest 100 dollars. 

{% enddocs %}

{% docs nado_hour %}

The hour in which the stats table data was pull and inserted into the table.

{% enddocs %}

{% docs nado_distinct_sequencer_batches %}

The amount of sequencer transactions that included this product in the last hour.

{% enddocs %}

{% docs nado_trader_count %}

The distinct traders in the last hour, based on a distinct count of wallet addresses.

{% enddocs %}

{% docs nado_subaccount_count %}

The distinct traders in the last hour, based on a distinct count of subaccount.

{% enddocs %}

{% docs nado_total_trade_count %}

The total number of trades on Nado in the last hour.

{% enddocs %}

{% docs nado_contract_price %}

The price of the contract when the data was inserted into the table.

{% enddocs %}

{% docs nado_base_volume_24h %}

The 24 hour trading volume for the pair (unit in base).

{% enddocs %}

{% docs nado_quote_volume_24h %}

The 24 hour trading volume for the pair (unit in quote).

{% enddocs %}

{% docs nado_funding_rate %}

Current 24hr funding rate. Can compute hourly funding rate dividing by 24.

A funding rate is a mechanism used to ensure that the price of a perp contract tracks the underlying asset's price as closely as possible.

Positive funding rates reflect the perpetual trading at a premium to the underlying asset’s price.

{% enddocs %}

{% docs nado_index_price %}

Last calculated index price for underlying of contract.
{% enddocs %}

{% docs nado_last_price %}

Last transacted price of base currency based on given quote currency.
{% enddocs %}


{% docs nado_mark_price %}

The calculated fair value of the contract, independent of the last traded price on the specific exchange. 
{% enddocs %}

{% docs nado_next_funding_rate %}

Timestamp of the next funding rate change, specific to hour the data was pulled from the API.
{% enddocs %}

{% docs nado_open_interest %}

The open interest of the contract for the hour that the data was pulled. Open interest (OI) refers to the total number of outstanding derivative contracts (e.g., futures or options) that are currently held by market participants and have not yet been settled
{% enddocs %}

{% docs nado_open_interest_usd %}

The open interest of the contract for the hour that the data was pulled, denominated in USD. Open interest (OI) refers to the total number of outstanding derivative contracts (e.g., futures or options) that are currently held by market participants and have not yet been settled
{% enddocs %}

{% docs nado_quote_currency %}

Symbol of the target asset.
{% enddocs %}

{% docs nado_stake_action %}

The staking action with the VRTX staking address
{% enddocs %}
