{% docs evm_swaps_amount0_adjusted %}

The delta of the token0 balance of the pool, decimal adjusted.

{% enddocs %}


{% docs evm_swaps_amount0_usd %}

The delta of the token0 balance of the pool, converted to USD

{% enddocs %}


{% docs evm_swaps_amount1_adjusted %}

The delta of the token1 balance of the pool, decimal adjusted.

{% enddocs %}


{% docs evm_swaps_amount1_usd %}

The delta of the token1 balance of the pool, converted to USD

{% enddocs %}


{% docs evm_all_liquidity %}

The liquidity of the pool.

{% enddocs %}


{% docs evm_all_liquidity_adjusted %}

The liquidity of the pool, decimal adjusted.

{% enddocs %}


{% docs evm_positions_liquidity_provider %}

The address of the LP

{% enddocs %}


{% docs evm_positions_nf_position_manager_address %}

The address of the peripheral nf position manager contract, if used. 

{% enddocs %}


{% docs evm_positions_nf_token_id %}

The id of the NFT associated with the liquidity position

{% enddocs %}


{% docs evm_all_pool_address %}

The contract address of the pool

{% enddocs %}


{% docs evm_all_pool_name %}

The contract label/name of the pool

{% enddocs %}


{% docs evm_swaps_price_0_1 %}

The amount of token0 per token1 that the swap occurred at

{% enddocs %}


{% docs evm_swaps_price_1_0 %}

The amount of token1 per token0 that the swap occurred at

{% enddocs %}


{% docs evm_positions_price_lower_0_1 %}

Lower bound of the liquidity position represented as token 0 per token 1

{% enddocs %}


{% docs evm_positions_price_lower_0_1_usd %}

Lower bound of the liquidity position represented as token 0 per token 1, converted to USD

{% enddocs %}


{% docs evm_positions_price_lower_1_0 %}

Lower bound of the liquidity position represented as token 1 per token 0

{% enddocs %}


{% docs evm_positions_price_lower_1_0_usd %}

Lower bound of the liquidity position represented as token 1 per token 0, converted to USD

{% enddocs %}


{% docs evm_positions_price_upper_0_1 %}

Upper bound of the liquidity position represented as token 0 per token 1

{% enddocs %}


{% docs evm_positions_price_upper_0_1_usd %}

Upper bound of the liquidity position represented as token 0 per token 1, converted to USD

{% enddocs %}


{% docs evm_positions_price_upper_1_0 %}

pper bound of the liquidity position represented as token 1 per token 0

{% enddocs %}


{% docs evm_positions_price_upper_1_0_usd %}

Upper bound of the liquidity position represented as token 1 per token 0, converted to USD

{% enddocs %}


{% docs evm_positions_tick_lower %}

Lower tick of the liquidity position

{% enddocs %}


{% docs evm_positions_tick_upper %}

Upper tick of the liquidity position

{% enddocs %}


{% docs evm_all_token0_address %}

Contract address of token 0

{% enddocs %}


{% docs evm_all_token0_decimals %}

Decimal adjustment of token0

{% enddocs %}


{% docs evm_all_token0_name %}

Name of token0

{% enddocs %}


{% docs evm_all_token0_price %}

Price of token0

{% enddocs %}


{% docs evm_all_token0_symbol %}

Symbol of token0

{% enddocs %}


{% docs evm_all_token1_address %}

Address of token1

{% enddocs %}


{% docs evm_all_token1_decimals %}

Decimal adjustment of token1

{% enddocs %}


{% docs evm_all_token1_name %}

Name of token1

{% enddocs %}


{% docs evm_all_token1_price %}

Price of token1

{% enddocs %}


{% docs evm_all_token1_symbol %}

Symbol of token1

{% enddocs %}


{% docs evm_lp_actions_action %}

The type of lp action, either INCREASE_LIQUIDITY (mint) or DECREASE_LIQUIDITY (burn)

{% enddocs %}


{% docs evm_lp_actions_table_doc %}

Use this table to track increases and decreases to positions by liquidity providers (LPs) over time. Whenever a Pool Burn or Mint event is triggered on a position a record is appended to this table.

{% enddocs %}


{% docs evm_pool_stats_fee_growth_global0_x128 %}

The fee growth as a Q128.128 fees of token0 collected per unit of liquidity for the entire life of the pool

{% enddocs %}


{% docs evm_pool_stats_fee_growth_global1_x128 %}

The fee growth as a Q128.128 fees of token1 collected per unit of liquidity for the entire life of the pool

{% enddocs %}


{% docs evm_pool_stats_protocol_fees_token0_adjusted %}

The amount of token0 owed to the protocol, decimal adjusted

{% enddocs %}


{% docs evm_pool_stats_protocol_fees_token1_adjusted %}

The amount of token1 owed to the protocol, decimal adjusted

{% enddocs %}


{% docs evm_pool_stats_table_doc %}

Statistics for each pool, appened each time a transaction triggers a Pool Event (i.e. 'Initialize', 'Mint', 'Collect', 'Burn', 'Swap', 'Flash', 'IncreaseObservationCardinalityNext', 'SetFeeProtocol', 'CollectProtocol', etc.). 

A new record is appended each time this occurs. These stats are read from the Pool contract state leveraging Flipside's fully archival Ethereum cluster.

{% enddocs %}


{% docs evm_pool_stats_tick %}

The tick of the pool according to the last tick transitions that was run.

{% enddocs %}


{% docs evm_pool_stats_token0_balance %}

The balance of token0 locked in the pool contract as of this block.

{% enddocs %}


{% docs evm_pool_stats_token0_balance_adjusted %}

The balance of token0 locked in the pool contract as of this block, decimal adjusted.

{% enddocs %}


{% docs evm_pool_stats_token0_balance_usd %}

The balance of token0 locked in the pool contract as of this block in USD.

{% enddocs %}


{% docs evm_pool_stats_token1_balance %}

The balance of token1 locked in the pool contract as of this block.

{% enddocs %}


{% docs evm_pool_stats_token1_balance_adjusted %}

The balance of token1 locked in the pool contract as of this block, decimal adjusted.

{% enddocs %}


{% docs evm_pool_stats_token1_balance_usd %}

The balance of token1 locked in the pool contract as of this block in USD.

{% enddocs %}


{% docs evm_pool_stats_unlocked %}

Whether the pool is currently locked to reentrancy

{% enddocs %}


{% docs evm_pool_stats_virtual_liquidity_adjusted %}

The virtual liquidity of the pool

{% enddocs %}


{% docs evm_pool_stats_virtual_reserves_token0_adjusted %}

The virtual reserves of token0, decimal adjusted, in the pool.

{% enddocs %}


{% docs evm_pool_stats_virtual_reserves_token0_usd %}

The virtual reserves of token0, converted to USD.

{% enddocs %}


{% docs evm_pool_stats_virtual_reserves_token1_adjusted %}

The virtual reserves of token1, decimal adjusted, in the pool.

{% enddocs %}


{% docs evm_pool_stats_virtual_reserves_token1_usd %}

The virtual reserves of token1, converted to USD.

{% enddocs %}


{% docs evm_pools_factory_address %}

The address of the UniswapV3 factory that initialized this Pool.

{% enddocs %}


{% docs evm_pools_fee %}

The swapping fee of the pool. Liquidity providers initially created pools at three fee levels: 0.05%, 0.30%, and 1%, though more fee levels have been added by UNI governance.

{% enddocs %}


{% docs evm_pools_fee_percent %}

The fee expressed as a decimal percentage

{% enddocs %}


{% docs evm_pools_init_price_1_0 %}

The initial price of the Pool (converted from sqrtPriceX96).

{% enddocs %}


{% docs evm_pools_init_price_1_0_usd %}

The initial price of the Pool (converted from sqrtPriceX96) in USD. 

{% enddocs %}


{% docs evm_pools_init_tick %}

The initial tick of the Pool

{% enddocs %}


{% docs evm_pools_table_doc %}

Pool records are appended to this table whenever a PoolCreated event is emitted by the UniswapV3 Factory Contract.

{% enddocs %}


{% docs evm_pools_tick_spacing %}

The minimum number of ticks allowed between each tick.

{% enddocs %}


{% docs evm_position_collected_fees_table_doc %}

Fees collected by a Liquidity Provider (LP) on their position. In V3 fees are accrued and collected in each token within the pair. When a pool Collect event is emitted a new record is appended to this table. If a Burn event is emitted in the same transaction as the Collect event the amount of the burn is subtracted from the Collect event token1 and token0 amounts. This allows us to arrive solely at the swap fees collected.

{% enddocs %}


{% docs evm_positions_collected_fees_event_index %}

Event index pertains to the grouping of individual events together. Withing one event index there could be multiple messages that take place and this would be the key to tie them together. 

{% enddocs %}


{% docs evm_positions_collected_fees_price_lower %}

Lower bound of the liquidity position represented as token 1 per token 0.

{% enddocs %}


{% docs evm_positions_collected_fees_price_lower_usd %}

Lower bound of the liquidity position represented as token 1 per token 0, converted to USD.

{% enddocs %}


{% docs evm_positions_collected_fees_price_upper %}

Upper bound of the liquidity position represented as token 1 per token 0.

{% enddocs %}


{% docs evm_positions_collected_fees_price_upper_usd %}

Upper bound of the liquidity position represented as token 1 per token 0, converted to USD.

{% enddocs %}


{% docs evm_positions_fee_growth_inside0_last_x128 %}

The fee growth of token0 as of the last action on the individual position.

{% enddocs %}


{% docs evm_positions_fee_growth_inside1_last_x128 %}

The fee growth of token1 as of the last action on the individual position.

{% enddocs %}


{% docs evm_positions_fee_percent %}

Percent of fees

{% enddocs %}


{% docs evm_positions_is_active %}

Is the position currently active? When a position is closed this is set to false.

{% enddocs %}


{% docs evm_positions_table_doc %}

Positions opened by liquidity providers at a specific tick range (tick_lower, tick_upper). Whenever an event is emitted related to a position a new record is added to this table with the latest state of the position.

{% enddocs %}


{% docs evm_positions_token_owed0_adjusted %}

The uncollected amount of token0 owed to the position as of the last computation.

{% enddocs %}


{% docs evm_positions_tokens_owed0_usd %}

The uncollected amount of token0 owed to the position as of the last computation, converted to USD.

{% enddocs %}


{% docs evm_positions_token_owed1_adjusted %}

The uncollected amount of token1 owed to the position as of the last computation.

{% enddocs %}


{% docs evm_positions_tokens_owed1_usd %}

The uncollected amount of token1 owed to the position as of the last computation, converted to USD.

{% enddocs %}


{% docs evm_swaps_log_index %}

Log index pertains to the grouping of individual events together. Withing one log index there could be multiple events that take place and this would be the key to tie them together. 

{% enddocs %}


{% docs evm_swaps_price %}

Price of swap

{% enddocs %}


{% docs evm_swaps_recipient %}

The address that received the output of the swap

{% enddocs %}


{% docs evm_swaps_sender %}

The address that initiated the swap call, and that received the callback

{% enddocs %}


{% docs evm_swaps_sqrt_price_x96 %}

Original swap price before conversion

{% enddocs %}


{% docs evm_swaps_table_doc %}

All swaps that occur on V3 pools.

{% enddocs %}


{% docs evm_swaps_tick %}

The log base 1.0001 of the price of the pool after the swap

{% enddocs %}


