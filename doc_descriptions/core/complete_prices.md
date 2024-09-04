{% docs evm_prices_address %}

The unique Ethereum address for a given token.  

{% enddocs %}

{% docs evm_prices_decimals %}

The number of decimals for token contract. 

{% enddocs %}

{% docs evm_prices_hour %}

  Hour at which the token price was recorded. 

{% enddocs %}

{% docs evm_prices_imputed %}

This column indicates whether or not the price has been pulled forward from the previous hour. Sometimes hourly prices are missed from our data source, and in that case we carry forward the last recorded hourly price until we record a new price. 

{% enddocs %}

{% docs evm_prices_price %}

The token price for a given hour.

{% enddocs %}

{% docs evm_prices_table_doc %}

This table contains hourly prices for tokens on the Ethereum Blockchain. 
The sources of this data are [CoinMarketCap](https://coinmarketcap.com/) and [CoinGecko](https://www.coingecko.com/).

{% enddocs %}

{% docs evm_prices_dim_asset_metadata_table_doc %}

A comprehensive dimensional table holding asset metadata and other relevant details pertaining to each id, from multiple providers. This data set includes raw, non-transformed data coming directly from the provider APIs and rows are not intended to be unique. As a result, there may be data quality issues persisting in the APIs that flow through to this dimensional model. If you are interested in using a curated data set instead, please utilize ez_asset_metadata.

{% enddocs %}

{% docs evm_prices_ez_asset_metadata_table_doc %}

A convenience table holding prioritized asset metadata and other relevant details pertaining to each token_address and native asset. This data set is highly curated and contains metadata for one unique asset per blockchain.

{% enddocs %}

{% docs evm_prices_fact_prices_ohlc_hourly_table_doc %}

A comprehensive fact table holding id and provider specific open, high, low, close hourly prices, from multiple providers. This data set includes raw, non-transformed data coming directly from the provider APIs and rows are not intended to be unique. As a result, there may be data quality issues persisting in the APIs that flow through to this fact based model. If you are interested in using a curated data set instead, please utilize ez_prices_hourly.

{% enddocs %}

{% docs evm_prices_ez_prices_hourly_table_doc %}

A convenience table for determining token prices by address and blockchain, and native asset prices by symbol and blockchain. This data set is highly curated and contains metadata for one price per hour per unique asset and blockchain.

{% enddocs %}

{% docs evm_prices_provider %}

The provider or source of the data.

{% enddocs %}

{% docs evm_prices_asset_id %}

The unique identifier representing the asset.

{% enddocs %}

{% docs evm_prices_name %}

The name of asset.

{% enddocs %}

{% docs evm_prices_symbol %}

The symbol of asset.

{% enddocs %}

{% docs evm_prices_token_address %}

The specific address representing the asset on a specific platform. This will be NULL if referring to a native asset.

{% enddocs %}

{% docs evm_prices_token_address_evm %}

The specific address representing the asset on a specific platform. This will be NULL if referring to a native asset. The case (upper / lower) may or may not be specified within the `dim_asset_metadata` table, as this column is raw and not transformed, coming directly from the provider APIs. However, in the `ez_` views, it will be lowercase by default for all EVMs.

{% enddocs %}

{% docs evm_prices_blockchain %}

The Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs evm_prices_blockchain_id %}

The unique identifier of the Blockchain, Network, or Platform for this asset.

{% enddocs %}

{% docs evm_prices_decimals %}

The number of decimals for the asset. May be NULL.

{% enddocs %}

{% docs evm_prices_is_native %}

A flag indicating assets native to the respective blockchain.

{% enddocs %}

{% docs evm_prices_is_deprecated %}

A flag indicating if the asset is deprecated or no longer supported by the provider.

{% enddocs %}

{% docs evm_prices_id_deprecation %}

Deprecating soon! Please use the `asset_id` column instead.

{% enddocs %}

{% docs evm_prices_decimals_deprecation %}

Deprecating soon! Please use the decimals column in `ez_asset_metadata` or join in `dim_contracts` instead.

{% enddocs %}

{% docs evm_prices_hour %}

Hour that the price was recorded at.

{% enddocs %}

{% docs evm_prices_price %}

Closing price of the recorded hour in USD.

{% enddocs %}

{% docs evm_prices_is_imputed %}

A flag indicating if the price was imputed, or derived, from the last arriving record. This is generally used for tokens with low-liquidity or inconsistent reporting.

{% enddocs %}

{% docs evm_prices_open %}

Opening price of the recorded hour in USD.

{% enddocs %}

{% docs evm_prices_high %}

Highest price of the recorded hour in USD

{% enddocs %}

{% docs evm_prices_low %}

Lowest price of the recorded hour in USD

{% enddocs %}

{% docs evm_prices_close %}

Closing price of the recorded hour in USD

{% enddocs %}

