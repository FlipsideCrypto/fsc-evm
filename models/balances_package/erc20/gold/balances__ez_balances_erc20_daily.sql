{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__balances_erc20') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'ez_balances_erc20_daily_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = [
        "{{ unverify_balances() }}",
        "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, contract_address)"
    ],
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','erc20','heal','phase_4']
) }}

WITH balances AS (

    SELECT
        s.block_number,
        block_date,
        s.address,
        s.contract_address,
        IFF(
            c.decimals IS NULL
            AND s.contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
            18,
            c.decimals
        ) AS decimals_adj,
        c.symbol,
        balance_hex,
        IFNULL(
            CASE WHEN LENGTH(balance_hex) <= 4300 AND balance_hex IS NOT NULL 
                THEN TRY_CAST(utils.udf_hex_to_int(balance_hex) AS bigint) 
            END,
            CASE WHEN balance_hex IS NOT NULL 
                THEN TRY_CAST(utils.udf_hex_to_int(RTRIM(balance_hex,'0')) AS bigint) 
            END
        ) AS balance_raw,
        IFF(
            decimals_adj IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                balance_raw,
                decimals_adj
            )
        ) AS balance_precise,
        balance_precise :: FLOAT AS balance,
        IFF(
            decimals_adj IS NULL,
            NULL,
            ROUND(
                balance * IFF(
                    s.contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
                    COALESCE(
                        p0.price,
                        p1.price
                    ),
                    p0.price
                ),
                2
            )
        ) AS balance_usd
    FROM
        {{ ref('silver__balances_erc20_daily') }}
        s
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p0
        ON s.contract_address = p0.token_address
        AND DATEADD(
            'hour',
            23,
            s.block_date
        ) = p0.hour
        AND p0.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p1
        ON DATEADD(
            'hour',
            23,
            s.block_date
        ) = p1.hour
        AND p1.is_native
        LEFT JOIN {{ ref('core__dim_contracts')}}
        c
        ON s.contract_address = c.address
    WHERE
        balance_raw IS NOT NULL

{% if is_incremental() %}
AND s.modified_timestamp >= (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01')
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        block_date,
        t.address,
        t.contract_address,
        IFF(
            c.decimals IS NULL
            AND t.contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
            18,
            c.decimals
        ) AS decimals_heal,
        c.symbol AS symbol_heal,
        balance_hex,
        balance_raw,
        IFF(
            decimals_heal IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                balance_raw,
                decimals_heal
            )
        ) AS balance_precise_heal,
        balance_precise_heal :: FLOAT AS balance_heal,
        IFF(
            decimals_heal IS NULL,
            NULL,
            ROUND(
                balance_heal * IFF(
                    t.contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
                    COALESCE(
                        p0.price,
                        p1.price
                    ),
                    p0.price
                ),
                2
            )
        ) AS balance_usd_heal
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p0
        ON t.contract_address = p0.token_address
        AND DATEADD(
            'hour',
            23,
            t.block_date
        ) = p0.hour
        AND p0.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p1
        ON DATEADD(
            'hour',
            23,
            t.block_date
        ) = p1.hour
        AND p1.is_native
        LEFT JOIN {{ ref('core__dim_contracts')}}
        c
        ON t.contract_address = c.address
    WHERE
        (
            t.balance_usd IS NULL
        AND (
            p0.price IS NOT NULL
            OR (
                t.contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
                AND p1.price IS NOT NULL
            )
        )
        )
        OR (
            t.decimals IS NULL
            AND t.symbol IS NULL
        )
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_date,
        address,
        contract_address,
        decimals_adj AS decimals,
        symbol,
        balance_hex,
        balance_raw,
        balance_precise,
        balance,
        balance_usd
    FROM
        balances

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_date,
    address,
    contract_address,
    decimals_heal AS decimals,
    symbol_heal AS symbol,
    balance_hex,
    balance_raw,
    balance_precise_heal AS balance_precise,
    balance_heal AS balance,
    balance_usd_heal AS balance_usd
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_date,
    address,
    contract_address,
    decimals,
    symbol,
    balance_hex,
    balance_raw,
    balance_precise,
    balance,
    balance_usd,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address','contract_address']
    ) }} AS ez_balances_erc20_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    FINAL 
    
{% if is_incremental() %}
qualify(ROW_NUMBER() over (PARTITION BY ez_balances_erc20_daily_id
ORDER BY
    modified_timestamp DESC)) = 1
{% endif %}
