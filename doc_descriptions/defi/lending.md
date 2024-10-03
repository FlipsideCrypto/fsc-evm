{% docs evm_complete_lending_borrows_table_doc %}

This table contains transactions where users borrowed assets across various lending protocols. In order to borrow assets, a user must first deposit their preferred asset and amount as collateral. Some protocols required trace-level curation; as a result, certain columns exclusive to event logs, such as event index, will contain NULL values.

{% enddocs %}

{% docs evm_complete_lending_deposits_table_doc %}

This table contains deposit transactions across various lending protocols. A user deposits their preferred asset and amount. After depositing, users earn passive income based on the market borrowing demand. Additionally, depositing allows users to borrow by using their deposited assets as collateral. Any interest earned by depositing funds helps offset the interest rate accumulated by borrowing. Some protocols required trace-level curation; as a result, certain columns exclusive to event logs, such as event index, will contain NULL values.

{% enddocs %}

{% docs evm_complete_lending_flashloans_table_doc %}

This table contains flash loan transactions across various lending protocols. Flash loans are a feature designed for developers, due to the technical knowledge required to execute one. Flash Loans allow you to borrow any available amount of assets without providing any collateral, as long as the liquidity is returned to the protocol within one block transaction.

{% enddocs %}

{% docs evm_complete_lending_liquidations_table_doc %}

This table contains transactions in which a borrower's collateral asset is liquidated across various lending protocols. Liquidations occur when a borrower's health factor goes below 1 due to their collateral value not properly covering their loan/debt value. This might happen when the collateral decreases in value or the borrowed debt increases in value against each other. This collateral vs loan value ratio is shown in the health factor. In a liquidation, up to 50% of a borrower's debt is repaid and that value + liquidation fee is taken from the collateral available, so after a liquidation the amount liquidated from one's debt is repaid.

{% enddocs %}

{% docs evm_complete_lending_repayments_table_doc %}

This table contains transactions in which a borrower repays their loan (debt) across various lending protocols. Loans are repaid in the same asset borrowed, plus accrued interest. Borrowers can pay back their loan based on the USD price as they can borrow any of the available stable coins (USDC, DAI, USDT, etc.). Some protocols required trace-level curation; as a result, certain columns exclusive to event logs, such as event index, will contain NULL values.

{% enddocs %}

{% docs evm_complete_lending_withdraws_table_doc %}

This table contains transactions in which a user withdraws liquidity across various lending protocols. Users need to make sure there is enough liquidity (not borrowed) in order to withdraw, if this is not the case, users need to wait for more liquidity from depositors or borrowers repaying. Some protocols required trace-level curation; as a result, certain columns exclusive to event logs, such as event index, will contain NULL values.

{% enddocs %}

{% docs evm_complete_lending_borrow_rate_mode %}

The rate mode the user is swapping from. Stable: 1, Variable: 2. Borrowers can switch between the stable and variable rate at any time. Stable rates act as a fixed rate in the short-term, but can be re-balanced in the long-term in response to changes in market conditions. The variable rate is the rate based on the offer and demand. The stable rate, as its name indicates, will remain pretty stable and its the best option to plan how much interest you will have to pay. The variable rate will change over time and could be the optimal rate depending on market conditions.

{% enddocs %}

{% docs evm_complete_lending_borrow_rate_stable %}

The stable interest rate for borrowing assets.

{% enddocs %}

{% docs evm_complete_lending_borrow_rate_variable %}

The variable interest rate for borrowing assets.

{% enddocs %}

{% docs evm_complete_lending_collateral_complete_lending_token %}

The interest bearing token that's burned when a liquidation occurs.

{% enddocs %}

{% docs evm_complete_lending_collateral_asset %}

The asset provided as collateral, which can be liquidated.

{% enddocs %}

{% docs evm_complete_lending_data_provider %}

The protocol data provider contract address.

{% enddocs %}

{% docs evm_complete_lending_debt_complete_lending_token %}

The interest bearing token representing the debt.

{% enddocs %}

{% docs evm_complete_lending_debt_asset %}

The debt asset, which the user borrowed.

{% enddocs %}

{% docs evm_complete_lending_debt_to_cover_amount %}

The amount of debt the user must cover.

{% enddocs %}

{% docs evm_complete_lending_debt_to_cover_amount_usd %}

The amount of debt the user must cover, valued in USD.

{% enddocs %}

{% docs evm_complete_lending_depositor_address %}

The depositor's address.

{% enddocs %}

{% docs evm_complete_lending_end_voting_period %}

The block number in which the voting period ends.

{% enddocs %}

{% docs evm_complete_lending_flashloan_amount %}

The amount of assets flash loaned.

{% enddocs %}

{% docs evm_complete_lending_flashloan_amount_usd %}

The value of the flash loan amount, in USD.

{% enddocs %}

{% docs evm_complete_lending_governance_contract %}

The governance contract address.

{% enddocs %}

{% docs evm_complete_lending_initiator_address %}

The address that initiated the flash loan.

{% enddocs %}

{% docs evm_complete_lending_issued_tokens %}

The amount of tokens that the user is depositing.

{% enddocs %}

{% docs evm_complete_lending_lending_pool_contract %}

The address of the lending pool.

{% enddocs %}

{% docs evm_complete_lending_liquidated_amount %}

The amount of asset liquidated.

{% enddocs %}

{% docs evm_complete_lending_liquidated_amount_usd %}

The value of the liquidated asset, in USD.

{% enddocs %}

{% docs evm_complete_lending_liquidator %}

The address that initiated the liquidation call.

{% enddocs %}

{% docs evm_complete_lending_market %}

The asset contract for the applicable market.

{% enddocs %}

{% docs evm_complete_lending_payer %}

The address that initiated the repayment.

{% enddocs %}

{% docs evm_complete_lending_premium_amount %}

The flash loan fee, changeable via the normal governance process, decimal adjusted.

{% enddocs %}

{% docs evm_complete_lending_premium_amount_usd %}

The flash loan fee, valued in USD.

{% enddocs %}

{% docs evm_complete_lending_proposal_id %}

The unique ID representing a proposal.

{% enddocs %}

{% docs evm_complete_lending_proposal_tx %}

The transaction confirming a proposal submission.

{% enddocs %}

{% docs evm_complete_lending_proposer %}

The user's address that submitted the proposal.

{% enddocs %}

{% docs evm_complete_lending_repayed_tokens %}

The amount of tokens repaid.

{% enddocs %}

{% docs evm_complete_lending_repayed_usd %}

The value of repaid tokens, in USD.

{% enddocs %}

{% docs evm_complete_lending_stable_debt_token_address %}

Debt tokens are interest-accruing tokens that are minted and burned on borrow and repay, representing a debt to the protocol with a stable interest rate.

{% enddocs %}

{% docs evm_complete_lending_start_voting_period %}

The block number in which the voting period begins.

{% enddocs %}

{% docs evm_complete_lending_status %}

The proposal's status.

{% enddocs %}

{% docs evm_complete_lending_supplied_usd %}

The value of the asset in USD that the user is depositing.

{% enddocs %}

{% docs evm_complete_lending_supply_rate %}

The interest rate for supplying assets to the protocol.

{% enddocs %}

{% docs evm_complete_lending_support %}

A value indicating their vote (For: true, Against: false).

{% enddocs %}

{% docs evm_complete_lending_target_address %}

The address receiving the flash loan.

{% enddocs %}

{% docs evm_complete_lending_targets %}

List of the targeted addresses by proposal transactions.

{% enddocs %}

{% docs evm_complete_lending_token %}

The interest bearing token contract.

{% enddocs %}

{% docs evm_complete_lending_total_liquidity_token %}

The total supply of liquidity tokens.

{% enddocs %}

{% docs evm_complete_lending_total_liquidity_usd %}

The total value of liquidity tokens, in USD.

{% enddocs %}

{% docs evm_complete_lending_total_stable_debt_token %}

The total supply of debt tokens, representing a debt to the protocol with a stable interest rate.

{% enddocs %}

{% docs evm_complete_lending_total_stable_debt_usd %}

The total USD value of debt tokens, representing a debt to the protocol with a stable interest rate.

{% enddocs %}

{% docs evm_complete_lending_total_variable_debt_token %}

The total supply of debt tokens, representing a debt to the protocol with a variable interest rate.

{% enddocs %}

{% docs evm_complete_lending_total_variable_debt_usd %}

The total USD value of debt tokens, representing a debt to the protocol with a variable interest rate.

{% enddocs %}

{% docs evm_complete_lending_utilization_rate %}

The percentage of assets loaned out.

{% enddocs %}

{% docs evm_complete_lending_variable_debt_token_address %}

Debt tokens are interest-accruing tokens that are minted and burned on borrow and repay, representing a debt to the protocol with a variable interest rate.

{% enddocs %}

{% docs evm_complete_lending_version %}

The contract version. Example: v1, v2

{% enddocs %}

{% docs evm_complete_lending_withdrawn_tokens %}

The amount of tokens withdrawn.

{% enddocs %}

{% docs evm_complete_lending_withdrawn_usd %}

The value of withdrawn tokens, in USD.

{% enddocs %}

{% docs evm_complete_lending_platform %}

The specific protocol where the lending event occurred.

{% enddocs %}

{% docs evm_complete_lending_protocol_token %}

The protocol's specific lending asset token.

{% enddocs %}

{% docs evm_complete_lending_borrower %}

Address that initiated the borrow event.

{% enddocs %}

{% docs evm_complete_lending_amount %}

The decimal adjusted amount of tokens involved in the lending transaction, where available.

{% enddocs %}

{% docs evm_complete_lending_amount_usd %}

The value of the tokens in USD at the time of the lending transaction, where available.

{% enddocs %}

{% docs evm_complete_lending_token_address %}

The address of the token associated with the lending action.

{% enddocs %}

{% docs evm_complete_lending_token_symbol %}

The symbol of the token associated with the lending action.

{% enddocs %}

{% docs evm_complete_lending_depositor %}

Address that initiated a deposit event.

{% enddocs %}

{% docs evm_complete_lending_amount_unadj %}

The non-decimal adjusted amount of tokens involved in the lending transaction.

{% enddocs %}

{% docs evm_complete_lending_premium_amount_unadj %}

The flash loan fee, changeable via the normal governance process, non-decimal adjusted.

{% enddocs %}

{% docs evm_complete_lending_flashloan_amount_unadj %}

The amount of assets flash loaned, non-decimal adjusted.

{% enddocs %}

{% docs evm_complete_lending_flashloan_token %}

The flashloaned token address.

{% enddocs %}

{% docs evm_complete_lending_flashloan_token_symbol %}

The flashloaned token symbol.

{% enddocs %}

{% docs evm_borrower %}

It's the address of the user who is borrowing or repaying the loan, depending on the action.

{% enddocs %}
