{% docs evm_aave_borrows_table_doc %}

Aave.ez_borrows contains transactions where users borrowed assets via the Aave protocol. In order to borrow assets, a user must first deposit their preferred asset and amount as collateral.  Borrowers can choose either a stable or variable borrow rate. For more information, see column descriptions. 


{% enddocs %}


{% docs evm_aave_deposits_table_doc %}

Aave.ez_deposits contains transactions where users deposited into the Aave protocol. A user deposits their preferred asset and amount. After depositing, users earn passive income based on the market borrowing demand. Additionally, depositing allows users to borrow by using their deposited assets as a collateral. Any interest earned by depositing funds helps offset the interest rate accumulated by borrowing.

{% enddocs %}


{% docs evm_aave_flashloans_table_doc %}

Aave.ez_flashloans contains flash loan transactions where a borrower executes an
undercollateralised, one-block liquidity loan. Flash loans are a feature designed for developers, due to the technical knowledge required to execute one. Flash Loans allow you to borrow any available amount of assets without providing any collateral, as long as the liquidity is returned to the protocol within one block transaction.  

{% enddocs %}


{% docs evm_aave_liquidations_table_doc %}

Aave.ez_liquidatons contains transactions in which a borrower's collateral asset is liquidated. Liquidations occur when a borrower's health factor goes below 1 due to their collateral value not properly covering their loan/debt value. This might happen when the collateral decreases in value or the borrowed debt increases in value against each other. This collateral vs loan value ratio is shown in the health factor. In a liquidation, up to 50% of a borrower's debt is repaid and that value + liquidation fee is taken from the collateral available, so after a liquidation the amount liquidated from one's debt is repaid. 

{% enddocs %}


{% docs evm_aave_market_stats_table_doc %}

Aave.ez_market_stats details market statistics for Aave markets by block hour.  These include reserves, token addresses, token prices, borrow and supply rates, the utilization rate, and total supplies of liquidity and debt tokens. For more information, see column descriptions.  

{% enddocs %}


{% docs evm_aave_proposals_table_doc %}

Aave.ez_proposals contains transactions in which Aave improvement proposals are submitted for governance voting.  AAVE and/or stkAAVE token holders receive governance powers. Proposal power gives access to creating and sustaining a proposal. 

{% enddocs %}


{% docs evm_aave_repayments_table_doc %}

Aave.ez_repayments contains transactions in which a borrower repays their loan (debt).  Loans are repaid in the same asset borrowed, plus accrued interest. Borrowers can also use their collateral to repay in version 2 of Aave Protocol. Borrowers can pay back their loan based on the USD price as they can borrow any of the available stable coins (USDC, DAI, USDT, etc.).

{% enddocs %}


{% docs evm_aave_votes_table_doc %}

Aave.ez_votes contains Aave governance voting transactions. AAVE and/or stkAAVE token holders receive governance powers proportionally to the sum of their balance. Voting power is used to vote for or against existing proposals.


{% enddocs %}


{% docs evm_aave_withdraws_table_doc %}

Aave.ez_withdraws contains transactions in which a user withdraws liquidity from the Aave protocol.  Users can use their “aTokens" as liquidity without withdrawing. They need to make sure there is enough liquidity (not borrowed) in order to withdraw, if this is not the case, users need to wait for more liquidity from depositors or borrowers repaying.

{% enddocs %}


{% docs evm_aave_borrow_rate_mode %}

The rate mode the user is swapping from. Stable: 1, Variable: 2.  Borrowers can switch between the stable and variable rate at any time. Stable rates act as a fixed rate in the short-term, but can be re-balanced in the long-term in response to changes in market conditions. The variable rate is the rate based on the offer and demand in Aave. The stable rate, as its name indicates, will remain pretty stable and its the best option to plan how much interest you will have to pay. The variable rate will change over time and could be the optimal rate depending on market conditions. 

{% enddocs %}


{% docs evm_aave_borrow_rate_stable %}

The stable interest rate for borrowing assets.

{% enddocs %}


{% docs evm_aave_borrow_rate_variable %}

The variable interest rate for borrowing assets.

{% enddocs %}


{% docs evm_aave_collateral_aave_token %}

The Aave interest bearing token that's burned when a liquidation occurs. 

{% enddocs %}


{% docs evm_aave_collateral_asset %}

The asset provided as collateral, which can be liquidated.

{% enddocs %}


{% docs evm_aave_data_provider %}

The Aave protocol data provider contract address. 

{% enddocs %}


{% docs evm_aave_debt_aave_token %}

The interest bearing Aave token representing the debt. 

{% enddocs %}


{% docs evm_aave_debt_asset %}

The debt asset, which the user borrowed. 

{% enddocs %}


{% docs evm_aave_debt_to_cover_amount %}

The amount of debt the user must cover.

{% enddocs %}


{% docs evm_aave_debt_to_cover_amount_usd %}

The amount of debt the user must cover, valued in USD. 

{% enddocs %}


{% docs evm_aave_depositor_address %}

The depositor's address.

{% enddocs %}


{% docs evm_aave_end_voting_period %}

The block number in which the voting period ends.

{% enddocs %}


{% docs evm_aave_flashloan_amount %}

The amount of assets flash loaned.  

{% enddocs %}


{% docs evm_aave_flashloan_amount_usd %}

The value of the flash loan amount, in USD. 

{% enddocs %}


{% docs evm_aave_governance_contract %}

The governance contract address.

{% enddocs %}


{% docs evm_aave_initiator_address %}

The address that initiated the flash loan.

{% enddocs %}


{% docs evm_aave_issued_tokens %}

The amount of tokens that the user is depositing.

{% enddocs %}


{% docs evm_aave_lending_pool_contract %}

The address of the lending pool. This changes based on the Aave version.

{% enddocs %}


{% docs evm_aave_liquidated_amount %}

The amount of asset liquidated.

{% enddocs %}


{% docs evm_aave_liquidated_amount_usd %}

The value of the liquidated asset, in USD.

{% enddocs %}


{% docs evm_aave_liquidator %}

The address that initiated the liquidation call. 

{% enddocs %}


{% docs evm_aave_market %}

The asset contract for the applicable Aave market.   

{% enddocs %}


{% docs evm_aave_payer %}

The address that initiated the repayment.

{% enddocs %}


{% docs evm_aave_premium_amount %}

The flash loan fee, currently 0.09%, changeable via the normal governance process.

{% enddocs %}


{% docs evm_aave_premium_amount_usd %}

The flash loan fee, valued in USD. 

{% enddocs %}


{% docs evm_aave_proposal_id %}

The unique ID representing a proposal.

{% enddocs %}


{% docs evm_aave_proposal_tx %}

The transaction confirming a proposal submission. 

{% enddocs %}


{% docs evm_aave_proposer %}

The user's address that submitted the proposal.

{% enddocs %}


{% docs evm_aave_repayed_tokens %}

The amount of tokens repaid. 

{% enddocs %}


{% docs evm_aave_repayed_usd %}

The value of repaid tokens, in USD.

{% enddocs %}


{% docs evm_aave_stable_debt_token_address %}

Debt tokens are interest-accruing tokens that are minted and burned on borrow and repay, representing a debt to the protocol with a stable interest rate.

{% enddocs %}


{% docs evm_aave_start_voting_period %}

The block number in which the voting period begins.

{% enddocs %}


{% docs evm_aave_status %}

The proposal's status.

{% enddocs %}


{% docs evm_aave_supplied_usd %}

The value of the asset in USD that the user is depositing.

{% enddocs %}


{% docs evm_aave_supply_rate %}

The interest rate for supplying assets to the protocol.

{% enddocs %}


{% docs evm_aave_support %}

A value indicating their vote (For: true, Against: false).

{% enddocs %}


{% docs evm_aave_target_address %}

The address receiving the flash loan. 

{% enddocs %}


{% docs evm_aave_targets %}

List of the targeted addresses by proposal transactions.

{% enddocs %}


{% docs evm_aave_token %}

The Aave interest bearing token contract.  

{% enddocs %}


{% docs evm_aave_total_liquidity_token %}

The total supply of liquidity tokens.

{% enddocs %}


{% docs evm_aave_total_liquidity_usd %}

The total value of liquidity tokens, in USD. 

{% enddocs %}


{% docs evm_aave_total_stable_debt_token %}

The total supply of debt tokens, representing a debt to the protocol with a stable interest rate.

{% enddocs %}


{% docs evm_aave_total_stable_debt_usd %}

The total USD value of debt tokens, representing a debt to the protocol with a stable interest rate.
{% enddocs %}


{% docs evm_aave_total_variable_debt_token %}

The total supply of debt tokens, representing a debt to the protocol with a variable interest rate.

{% enddocs %}


{% docs evm_aave_total_variable_debt_usd %}

The total USD value of debt tokens, representing a debt to the protocol with a variable interest rate.
{% enddocs %}


{% docs evm_aave_utilization_rate %}

The percentage of assets loaned out.

{% enddocs %}


{% docs evm_aave_variable_debt_token_address %}

Debt tokens are interest-accruing tokens that are minted and burned on borrow and repay, representing a debt to the protocol with a variable interest rate.

{% enddocs %}


{% docs evm_aave_version %}

The contract version of Aave. Example: Aave AMM, Aave v1, Aave v2

{% enddocs %}


{% docs evm_aave_voter %}

The voter's address. 

{% enddocs %}


{% docs evm_aave_voting_power %}

The voter's voting power proportional to the sum of their balance of AAVE and/or stkAAVE. 

{% enddocs %}


{% docs evm_aave_withdrawn_tokens %}

The amount of tokens withdrawn. 

{% enddocs %}


{% docs evm_aave_withdrawn_usd %}

The value of withdrawn tokens, in USD.

{% enddocs %}


{% docs evm_stkaave_rate_supply %}

The stkAAVE rate for supplying assets to the protocol.

{% enddocs %}


{% docs evm_stkaave_rate_variable_borrow %}

The stkAAVE variable rate for borrowing assets.

{% enddocs %}


