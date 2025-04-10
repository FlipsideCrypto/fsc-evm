{% docs evm_current_bal_table_doc %}

This table contains the current, non-zero balances for wallets on this EVM blockchain. Symbol, name, and price are joined where possible. Prices are calculated as of the last activity date and as of the most recently recorded hourly price. ERC721s are included.

Please note - the underlying data for this is large. If you want your query to run quickly, please use filters as much as possible. Using at least `last_activity_block_timestamp::date` as a filter will lead to optimal query performance.

{% enddocs %}


{% docs evm_daily_balances_table_doc %}

READ THIS!

TLDR - You MUST use filters when interacting with this view. At a minimum, the `balance_date` must be filtered. Use `ez_balance_diffs` if possible. Only activity 2020+ is included.

This view contains the average daily balance of every wallet, for every token, including ERC721s. Previous balances are carried forward on days without activity.

This is an absolutely massive view, which is why filters must be applied if you do not want your query to time out. Balance_date is a requirement, but other filters will also be helpful for query performance. Data before 2020 has been excluded for performance sake. Wallets with activity before Jan 1 2020 may be represented incorrectly. You can find these older records in `ez_balance_diffs`.

`ez_balance_diffs` will have the best query performance of the balances tables, please use it if possible.

{% enddocs %}


{% docs evm_diffs_table_doc %}

This table contains the block level balance changes for both tokens (including ERC721s) and the native asset on this EVM blockchain for all wallets and contracts. If a token or the native asset is moved, we will read the balance of the involved wallets at that block, and carry forward the previous balance into the current record. Symbol, name, and price are joined where possible. ERC721s are included. 

Please note - the underlying data for this is large. If you want your query to run quickly, please use filters as much as possible. For optimal query performance, filter by `block_timestamp::date`.

If you want to take this data and make it daily, you can do so with the query below. You must use a `block_timestamp::date` filter here at a minimum. Other filters will help query runtime. 

```sql
WITH base_table AS (
    SELECT
        block_timestamp :: DATE AS balance_date,
        CASE
            WHEN symbol = 'ETH' THEN 'ETH'
            ELSE contract_address
        END AS contract_address,
        user_address,
        symbol,
        current_bal
    FROM
        ethereum.core.ez_balance_diffs
    WHERE
        block_timestamp :: DATE >= '' --user input 
        AND user_address = '' --user input
        AND (
            contract_address = '' --user input1
            OR symbol = 'ETH'
        )
),
all_days AS (
    SELECT
        date_day AS balance_date
    FROM
        ethereum.core.dim_dates
),
address_ranges AS (
    SELECT
        user_address,
        contract_address,
        symbol,
        MIN(
            balance_date :: DATE
        ) AS min_block_date,
        CURRENT_DATE() :: DATE AS max_block_date
    FROM
        base_table
    GROUP BY
        user_address,
        contract_address,
        symbol,
        max_block_date
),
all_dates AS (
    SELECT
        C.balance_date,
        A.user_address,
        A.contract_address,
        A.symbol
    FROM
        all_days C
        LEFT JOIN address_ranges A
        ON C.full_balance_date BETWEEN A.min_block_date
        AND A.max_block_date
    WHERE
        A.user_address IS NOT NULL
),
eth_balances AS (
    SELECT
        user_address,
        contract_address,
        balance_date,
        current_bal,
        TRUE AS daily_activity
    FROM
        base_table
),
balance_tmp AS (
    SELECT
        d.balance_date,
        d.user_address,
        d.contract_address,
        d.symbol,
        b.current_bal,
        b.daily_activity
    FROM
        all_dates d
        LEFT JOIN eth_balances b
        ON d.balance_date = b.balance_date
        AND d.user_address = b.user_address
        AND d.contract_address = b.contract_address
),
FINAL AS (
    SELECT
        balance_date,
        user_address,
        contract_address,
        symbol,
        LAST_VALUE(
            current_bal ignore nulls
        ) over(
            PARTITION BY user_address,
            contract_address
            ORDER BY
                balance_date ASC rows unbounded preceding
        ) AS balance,
        CASE
            WHEN daily_activity IS NULL THEN FALSE
            ELSE TRUE
        END AS daily_activity
    FROM
        balance_tmp
)
SELECT
    *
FROM
    FINAL
WHERE
    balance <> 0
ORDER BY
    balance_date DESC,
    contract_address
```
{% enddocs %}


{% docs evm_has_decimal %}

Boolean flag indicating whether token decimal information is available for this token (TRUE or FALSE).

{% enddocs %}


{% docs evm_has_price %}

Boolean flag indicating whether hourly price data for this token is available (TRUE or FALSE).

{% enddocs %}

{% docs evm_current_balances_last_activity_block %}

The last block where this token was transferred by this address.

{% enddocs %}


{% docs evm_current_balances_block_timestamp %}

The last block timestamp where this token was transferred by this address.

{% enddocs %}


{% docs evm_current_balances_user_address %}

The wallet address holding the tokens / native asset.

{% enddocs %}


{% docs evm_current_balances_contract_address %}

The contract address of the token (null for native asset).

{% enddocs %}


{% docs evm_current_balances_current_bal_unadj %}

The current raw token or native asset balance for this address, without a decimal adjustment.

{% enddocs %}


{% docs evm_current_balances_current_bal %}

The current decimal adjusted token or native asset balance.

{% enddocs %}


{% docs evm_current_balances_usd_value_last_activity %}

The value of the tokens or native asset in USD, at the time the last token activity occurred. Will be null for tokens without a decimal.

{% enddocs %}


{% docs evm_current_balances_usd_value_now %}

The value of the tokens or native asset in USD, as of the most recently recorded hourly price. Will be null for tokens without a decimal.

{% enddocs %}


{% docs evm_current_balances_symbol %}

The symbol of the token contract, or native asset. Please note this is not necessarily unique. 

{% enddocs %}


{% docs evm_current_balances_token_name %}

The name of the token contract, or native asset. Please note this is not necessarily unique. 

{% enddocs %}


{% docs evm_current_balances_decimals %}

The number of decimal places specified by the token contract for representing token amounts.

{% enddocs %}


{% docs evm_current_balances_has_decimal %}

Boolean flag indicating whether token decimal information is available for this token (TRUE or FALSE).

{% enddocs %}


{% docs evm_current_balances_has_price %}

Boolean flag indicating whether hourly price data for this token is available (TRUE or FALSE).

{% enddocs %}


{% docs evm_current_balances_last_recorded_price %}

The timestamp of the last hourly price recorded for this token.

{% enddocs %}

{% docs evm_balance_deltas_block_number %}

Block at which the balance was read (when the transfer occurred).

{% enddocs %}


{% docs evm_balance_deltas_block_timestamp %}

Block timestamp at which the balance was read (when the transfer occurred).

{% enddocs %}


{% docs evm_balance_deltas_user_address %}

The wallet address holding the tokens / native asset.

{% enddocs %}


{% docs evm_balance_deltas_contract_address %}

The contract address of the token (null for native asset).

{% enddocs %}


{% docs evm_balance_deltas_prev_bal_unadj %}

The token or native asset balance from the previously recorded record for this wallet / token, without a decimal adjustment.

{% enddocs %}


{% docs evm_balance_deltas_prev_bal %}

The decimal adjusted token or native asset balance from the previously recorded record for this wallet and token.

{% enddocs %}


{% docs evm_balance_deltas_prev_bal_usd %}

Previously recorded balance in USD - this will be null for tokens without a decimal adjustment. Please note, the USD value is calculated at this block.

{% enddocs %}


{% docs evm_balance_deltas_current_bal_unadj %}

The token or native asset balance at the current block number, without a decimal adjustment.

{% enddocs %}


{% docs evm_balance_deltas_current_bal %}

The decimal adjusted token or native asset balance at the current block number.

{% enddocs %}


{% docs evm_balance_deltas_current_bal_usd %}

The current balance in USD - this will be null for tokens without a decimal adjustment. Please note, the USD value is calculated at this block.

{% enddocs %}


{% docs evm_bal_delta_unadj %}

The non-decimal adjusted balance change.

{% enddocs %}


{% docs evm_bal_delta %}

The decimal adjusted balance change.

{% enddocs %}


{% docs evm_bal_delta_usd %}

The balance change in USD, will be null for tokens without a decimal.

{% enddocs %}