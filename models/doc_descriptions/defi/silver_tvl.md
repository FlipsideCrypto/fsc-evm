{% docs aave_v1_tvl_table_doc %}

Methodology:
Sums all underlying token balances held by the LendingPoolCore contract + native ETH in the protocol

{% enddocs %}

{% docs aave_v2_tvl_table_doc %}

Methodology:
Sums all underlying token balances held by each aToken contract (+ stkAAVE staking on Ethereum)

{% enddocs %}

{% docs aave_v3_tvl_table_doc %}

Methodology:
Sums totalSupply of each aToken contract

{% enddocs %}

{% docs binance_v1_tvl_table_doc %}

Methodology:
Total supply of wBETH

{% enddocs %}

{% docs curve_v1_tvl_table_doc %}

Methodology:
Sums all token balances held across pool contracts

{% enddocs %}

{% docs etherfi_v1_tvl_table_doc %}

Methodology:
Sums values from protocol oracle (minus looped positions), eUSD supply, BTC product balances (WBTC, LBTC, cbBTC) and ETHFI staking where applicable.

{% enddocs %}

{% docs lido_v1_tvl_table_doc %}

Methodology:
Total pooled assets reported by the Lido staking contracts

{% enddocs %}

{% docs tornado_cash_v1_tvl_table_doc %}

Methodology:
Sums ERC20 token balances + native asset balances across all mixer contracts

{% enddocs %}

{% docs uniswap_v1_tvl_table_doc %}

Methodology:
Sums native ETH balance across all pool contracts, doubled to represent total pool value (all pools are 50/50 ETH-token pairs)

{% enddocs %}

{% docs uniswap_v2_tvl_table_doc %}

Methodology:
Sums getReserves() values for token0 and token1 across all pools where both tokens are verified. This filtering removes spam/low-liquidity pools to provide a more accurate TVL value.

{% enddocs %}

{% docs uniswap_v3_tvl_table_doc %}

Methodology:
Sums token0 and token1 balances held by each pool contract where both tokens are verified. This filtering removes spam/low-liquidity pools to provide a more accurate TVL value.

{% enddocs %}

{% docs uniswap_v4_tvl_table_doc %}

Methodology:
Sums token0 and token1 balances held by the PoolManager singleton + any associated hook contracts where both tokens are verified. This filtering removes spam/low-liquidity pools to provide a more accurate TVL value.

{% enddocs %}

{% docs polymarket_v1_tvl_table_doc %}

Methodology:
Sums USDC balances held by the Conditional Tokens and Collateral Tokens contracts

{% enddocs %}

{% docs aerodrome_v1_tvl_table_doc %}

Methodology:
Sums getReserves() values for token0 and token1 across all AMM pools

{% enddocs %}

{% docs superchain_slipstream_v1_tvl_table_doc %}

Methodology:
Sums token0 and token1 balances held by each Slipstream (CL) pool contract via balanceOf()

{% enddocs %}

{% docs eigenlayer_v1_tvl_table_doc %}

Methodology:
Sums (1) native ETH restaked via EigenPods by matching PodDeployed events with beacon chain validator balances and (2) LST/ERC20 tokens deposited in strategy contracts via totalShares()

{% enddocs %}

{% docs ethena_v1_tvl_table_doc %}

Methodology:
Total supply of USDe (synthetic dollar)

{% enddocs %}

{% docs morpho_blue_v1_tvl_table_doc %}

Methodology:
Sums balanceOf each unique token (collateralToken + loanToken from CreateMarket events) held by the MorphoBlue singleton contract

{% enddocs %}