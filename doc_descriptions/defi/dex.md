{% docs evm_ez_dex_swaps_table_doc %}

This table contains swap events from the `fact_event_logs` table for popular decentralized exchanges (DEXes) on this EVM blockchain. It includes additional columns such as the amount in USD where possible. 
Note: A rule is applied to nullify the `amount_USD` if there is a significant divergence between `amount_in_USD` and `amount_out_usd`, which can occur during high price fluctuations for less liquid tokens.

Below are the available blockchains and their corresponding protocols:

| BLOCKCHAIN | PROTOCOLS                                                                                       |
|------------|-------------------------------------------------------------------------------------------------|
| ARBITRUM   | balancer, camelot-v2, camelot-v3, curve, dodo-v1, dodo-v2, gmx, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, ramses-v2, sparta, sushiswap, trader-joe-v1, uniswap-v2, uniswap-v3, zyberswap-v2, zyberswap-v3 |
| AVALANCHE  | balancer, curve, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, pangolin, pharaoh-v1, pharaoh-v2, platypus, sushiswap, trader-joe-v1, trader-joe-v2, uniswap-v3, woofi |
| BASE       | aerodrome, aerodrome-slipstream, alienbase, balancer, baseswap, dackieswap, kyberswap-v2, maverick, sushiswap, swapbased, uniswap-v2, uniswap-v3, voodoo, woofi |
| BSC        | biswap, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v2, level-finance, pancakeswap-v2, pancakeswap-v3, sushiswap, trader-joe-v1, uniswap-v2, uniswap-v3, woofi |
| ETHEREUM   | balancer, curve, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, maverick, pancakeswap-v3, shibaswap, sushiswap, synthetix, trader-joe-v2, uniswap-v2, uniswap-v3, verse |
| GNOSIS     | balancer, curve, honeyswap, sushiswap, swapr |
| KAIA       | capybara, dragonswap-v2, dragonswap-v3, kaiaswap, klayswap-v2, klayswap-v3, neopin |
| OPTIMISM   | balancer, beethoven-x, curve, dodo-v2, hashflow-v3, kyberswap-v1, kyberswap-v2, synthetix, uniswap-v2, uniswap-v3, velodrome, velodrome-v2, woofi |
| POLYGON    | balancer, curve, dodo-v2, fraxswap, hashflow-v3, kyberswap-v1, kyberswap-v2, quickswap-v2, quickswap-v3, sushiswap, uniswap-v3, woofi |

{% enddocs %}

{% docs evm_dex_lp_table_doc %}

This table provides details on decentralized exchange (DEX) liquidity pools (LP) on this EVM blockchain. It includes information on tokens, symbols, and decimals within each pool, applicable to various protocols.

Below are the available blockchains and their corresponding protocols:

| BLOCKCHAIN | PROTOCOLS                                                                                       |
|------------|-------------------------------------------------------------------------------------------------|
| ARBITRUM   | balancer, camelot-v2, camelot-v3, curve, dodo-v1, dodo-v2, gmx, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, ramses-v2, sparta, sushiswap, trader-joe-v1, uniswap-v2, uniswap-v3, zyberswap-v2, zyberswap-v3 |
| AVALANCHE  | balancer, curve, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, pangolin, pharaoh-v1, pharaoh-v2, platypus, sushiswap, trader-joe-v1, trader-joe-v2, uniswap-v3, woofi |
| BASE       | aerodrome, aerodrome-slipstream, alienbase, balancer, baseswap, dackieswap, kyberswap-v2, maverick, sushiswap, swapbased, uniswap-v2, uniswap-v3, voodoo, woofi |
| BSC        | biswap, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v2, level-finance, pancakeswap-v2, pancakeswap-v3, sushiswap, trader-joe-v1, uniswap-v2, uniswap-v3, woofi |
| ETHEREUM   | balancer, curve, dodo-v1, dodo-v2, fraxswap, hashflow, hashflow-v3, kyberswap-v1, kyberswap-v2, maverick, pancakeswap-v3, shibaswap, sushiswap, synthetix, trader-joe-v2, uniswap-v2, uniswap-v3, verse |
| GNOSIS     | balancer, curve, honeyswap, sushiswap, swapr |
| KAIA       | capybara, dragonswap-v2, dragonswap-v3, kaiaswap, klayswap-v2, klayswap-v3, neopin |
| OPTIMISM   | balancer, beethoven-x, curve, dodo-v2, hashflow-v3, kyberswap-v1, kyberswap-v2, synthetix, uniswap-v2, uniswap-v3, velodrome, velodrome-v2, woofi |
| POLYGON    | balancer, curve, dodo-v2, fraxswap, hashflow-v3, kyberswap-v1, kyberswap-v2, quickswap-v2, quickswap-v3, sushiswap, uniswap-v3, woofi |


{% enddocs %}

{% docs evm_dex_lp_deprecation %}

Deprecating soon: This table will be upgraded to include liquidity pools for all applicable DEXes on this EVM blockchain. Please migrate queries to `defi.dim_dex_liquidity_pools`. This table will be deprecated on 08/17/2023.

{% enddocs %}

{% docs evm_dex_creation_block %}

The block number at which this liquidity pool was created on the blockchain.

{% enddocs %}

{% docs evm_dex_creation_time %}

The timestamp of the block when this liquidity pool was created.

{% enddocs %}

{% docs evm_dex_creation_tx %}

The transaction that created this liquidity pool contract.

{% enddocs %}

{% docs evm_dex_factory_address %}

The address that deployed this liquidity pool, where available.

{% enddocs %}

{% docs evm_dex_lp_decimals %}

The number of decimals for the tokens included in the liquidity pool, represented as a JSON object, where available.

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM ethereum.defi.dim_dex_liquidity_pools
WHERE token0_decimal = 6
;

{% enddocs %}

{% docs evm_dex_lp_symbols %}

The symbol for the token included in the liquidity pool, as a JSON object, where available. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM ethereum.defi.dim_dex_liquidity_pools
WHERE token0_symbol = 'WETH'
;

{% enddocs %}

{% docs evm_dex_lp_tokens %}

The address for the token included in the liquidity pool, as a JSON object. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM ethereum.defi.dim_dex_liquidity_pools
WHERE token0 = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
;

{% enddocs %}

{% docs evm_dex_platform %}

The protocol or platform that the liquidity pool belongs to or swap occurred on. 

{% enddocs %}

{% docs evm_dex_pool_address %}

The contract address for the liquidity pool. 

{% enddocs %}

{% docs evm_dex_pool_name %}

The name of the liquidity pool, where available. In some cases, the pool name is a concatenation of symbols or token addresses.

{% enddocs %}

{% docs evm_dex_swaps_amount_in %}

The amount of tokens put into the swap.

{% enddocs %}

{% docs evm_dex_swaps_amount_in_unadj %}

The non-decimal adjusted amount of tokens put into the swap.

{% enddocs %}

{% docs evm_dex_swaps_amount_in_usd %}

The amount of tokens put into the swap converted to USD using the price of the token.

{% enddocs %}

{% docs evm_dex_swaps_amount_out %}

The amount of tokens taken out of or received from the swap.

{% enddocs %}

{% docs evm_dex_swaps_amount_out_unadj %}

The non-decimal adjusted amount of tokens taken out of or received from the swap.

{% enddocs %}

{% docs evm_dex_swaps_amount_out_usd %}

The amount of tokens taken out of or received from the swap converted to USD using the price of the token.

{% enddocs %}

{% docs evm_dex_swaps_sender %}

The Router is the Sender in the swap function. 

{% enddocs %}

{% docs evm_dex_swaps_symbol_in %}

The symbol of the token sent for swap.

{% enddocs %}

{% docs evm_dex_swaps_symbol_out %}

The symbol of the token being swapped to.

{% enddocs %}

{% docs evm_dex_swaps_token_in %}

The address of the token sent for swap.

{% enddocs %}

{% docs evm_dex_swaps_token_out %}

The address of the token being swapped to.

{% enddocs %}

{% docs evm_dex_swaps_tx_to %}

The tx_to is the address who receives the swapped token. This corresponds to the "to" field in the swap function.

{% enddocs %}

{% docs evm_dex_token0 %}

Token 0 is the first token in the pair, and will show up first within the event logs for relevant transactions. 

{% enddocs %}

{% docs evm_dex_token1 %}

Token 1 is the second token in the pair, and will show up second within the event logs for relevant transactions. 

{% enddocs %}

