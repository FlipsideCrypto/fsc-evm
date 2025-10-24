{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_locked_in_bridges_id"],
    cluster_by = ['block_date'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

WITH verified_stablecoins AS (

    SELECT
        token_address AS contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),
bridge_vault_list AS (
    SELECT
        DISTINCT bridge_address AS address
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    UNION
    SELECT
        vault_address AS address
    FROM
        {{ ref('silver_stablecoins__bridge_vault_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
),
raw_balances AS (
    SELECT
        block_date :: TIMESTAMP AS days,
        address,
        contract_address,
        symbol,
        balance
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN verified_stablecoins USING (contract_address)
        INNER JOIN bridge_vault_list USING (address)

{% if is_incremental() %}
WHERE
    block_date > (
        SELECT
            MAX(block_date)
        FROM
            {{ this }}
    )
{% endif %}
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_stablecoins AS (
    SELECT
        token_address AS contract_address
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),
newly_verified_stablecoins_raw_balances AS (
    SELECT
        block_date :: TIMESTAMP AS days,
        address,
        contract_address,
        symbol,
        balance
    FROM
        {{ ref('balances__ez_balances_erc20_daily') }}
        INNER JOIN newly_verified_stablecoins USING (contract_address)
        INNER JOIN bridge_vault_list USING (address)
),
{% endif %}

dates AS (
    SELECT
        date_day
    FROM
        {{ source(
            'crosschain_gold',
            'dim_dates'
        ) }}
    WHERE
        date_day BETWEEN

{% if is_incremental() and not var(
    'HEAL_MODEL',
    false
) %}
(
    SELECT
        MAX(block_date) + INTERVAL '1 day'
    FROM
        {{ this }}
)
{% else %}
    '2025-06-10'
{% endif %}
AND (
    SELECT
        MAX(days)
    FROM
        raw_balances
)
),
new_address_token_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1)
    FROM
        raw_balances
    GROUP BY
        ALL
),

{% if is_incremental() %}
past_address_token_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1)
    FROM
        {{ this }}
    GROUP BY
        ALL
),
{% endif %}

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_address_token_list AS (
    SELECT
        address,
        contract_address,
        COUNT(1)
    FROM
        newly_verified_stablecoins_raw_balances
    GROUP BY
        ALL
),
{% endif %}

complete_address_token_list AS (
    SELECT
        address,
        contract_address
    FROM
        new_address_token_list

{% if is_incremental() %}
UNION
SELECT
    address,
    contract_address
FROM
    past_address_token_list
{% endif %}

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    address,
    contract_address
FROM
    newly_verified_address_token_list
{% endif %}
),
full_list AS (
    SELECT
        date_day AS days,
        address,
        contract_address
    FROM
        dates
        CROSS JOIN complete_address_token_list
),

{% if is_incremental() %}
prev_balances AS (
    SELECT
        block_date,
        address,
        contract_address,
        symbol,
        balances,
        'old' AS TYPE
    FROM
        {{ this }}
        qualify ROW_NUMBER() over (
            PARTITION BY address,
            contract_address
            ORDER BY
                block_date DESC
        ) = 1
),
{% endif %}

balances_dates AS (
    SELECT
        f.days AS block_date,
        f.address,
        f.contract_address,
        symbol,
        balance,
        'new' AS TYPE
    FROM
        full_list f
        LEFT JOIN raw_balances b USING (
            days,
            address,
            contract_address
        )
),
all_balances AS (
    SELECT
        block_date,
        address,
        contract_address,
        symbol,
        balance,
        TYPE
    FROM
        balances_dates

{% if is_incremental() %}
UNION ALL
SELECT
    block_date,
    address,
    contract_address,
    symbol,
    balances AS balance,
    TYPE
FROM
    prev_balances
{% endif %}
),
FINAL AS (
    SELECT
        block_date,
        address,
        contract_address,
        symbol,
        TYPE,
        IFF(balance IS NULL, LAG(balance) ignore nulls over (PARTITION BY address, contract_address
    ORDER BY
        block_date ASC), balance) AS balances
    FROM
        all_balances
)
SELECT
    block_date,
    address,
    contract_address,
    symbol,
    balances,
    {{ dbt_utils.generate_surrogate_key(['block_date','address','contract_address']) }} AS stablecoins_locked_in_bridges_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL
WHERE
    TYPE = 'new'
