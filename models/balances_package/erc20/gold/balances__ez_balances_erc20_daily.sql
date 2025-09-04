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
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, contract_address)",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','erc20','phase_4']
) }}


WITH bronze AS (

    SELECT
        VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
        (
            VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
        ) :: DATE AS block_date,
        VALUE :"ADDRESS" :: STRING AS address,
        VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
        CASE
            WHEN LENGTH(
                DATA :result :: STRING
            ) <= 4300
            AND DATA :result IS NOT NULL THEN DATA :result :: STRING
            ELSE NULL
        END AS balance_hex
    FROM

{% if is_incremental() %}
{{ ref('bronze__balances_erc20') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    AND DATA :result :: STRING <> '0x'
{% else %}
    {{ ref('bronze__balances_erc20_fr') }}
WHERE
    DATA :result :: STRING <> '0x'
{% endif %}
),
balances AS (
    SELECT
        block_number,
        block_date,
        address,
        contract_address,
        IFF(
            p0.decimals IS NULL
            AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
            18,
            p0.decimals
        ) AS decimals_adj,
        p0.symbol,
        balance_hex,
        utils.udf_hex_to_int(balance_hex) AS balance_raw,
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
                    contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
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
        bronze b
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p0
        ON b.contract_address = p0.token_address
        AND DATEADD(
            'hour',
            23,
            block_date
        ) = p0.hour
        AND p0.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p1
        ON DATEADD(
            'hour',
            23,
            block_date
        ) = p1.hour
        AND p1.is_native
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        block_number,
        block_date,
        address,
        contract_address,
        IFF(
            p0.decimals IS NULL
            AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
            18,
            p0.decimals
        ) AS decimals_heal,
        p0.symbol AS symbol_heal,
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
                    contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}',
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
        ON b.contract_address = p0.token_address
        AND DATEADD(
            'hour',
            23,
            block_date
        ) = p0.hour
        AND p0.decimals IS NOT NULL
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p1
        ON DATEADD(
            'hour',
            23,
            block_date
        ) = p1.hour
        AND p1.is_native
    WHERE
        t.balance_usd IS NULL
        AND (
            p0.price IS NOT NULL
            OR (
                contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
                AND p1.price IS NOT NULL
            )
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
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY ez_balances_erc20_daily_id
ORDER BY
    modified_timestamp DESC)) = 1
