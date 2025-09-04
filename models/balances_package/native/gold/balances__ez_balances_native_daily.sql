{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends on: {{ ref('bronze__balances_native') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'ez_balances_native_daily_id',
    cluster_by = ['block_date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address)",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = vars.GLOBAL_GOLD_FR_ENABLED,
    tags = ['gold','balances','native','phase_4']
) }}

WITH bronze AS (

    SELECT
        VALUE :"BLOCK_NUMBER" :: NUMBER AS block_number,
        (
            VALUE :"BLOCK_DATE_UNIX" :: TIMESTAMP
        ) :: DATE AS block_date,
        VALUE :"ADDRESS" :: STRING AS address,
        DATA :result :: STRING AS balance_hex
    FROM

{% if is_incremental() %}
{{ ref('bronze__balances_native') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    AND DATA :result :: STRING <> '0x'
{% else %}
    {{ ref('bronze__balances_native_fr') }}
WHERE
    DATA :result :: STRING <> '0x'
{% endif %}
),
balances AS (
    SELECT
        block_number,
        block_date,
        address,
        balance_hex,
        utils.udf_hex_to_int(
            balance_hex
        ) :: bigint AS balance_raw,
        18 AS decimals,
        utils.udf_decimal_adjust(
            balance_raw,
            18
        ) AS balance_precise,
        balance_precise :: FLOAT AS balance,
        ROUND(balance * COALESCE(p0.price, p1.price), 2) AS balance_usd
    FROM
        bronze
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p0
        ON DATEADD(
            'hour',
            23,
            block_date
        ) = p0.hour --last hourly price of the day
        AND p0.token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
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
        t.decimals,
        balance_hex,
        balance_raw,
        balance_precise,
        balance,
        ROUND(balance * COALESCE(p0.price, p1.price), 2) AS balance_usd_heal
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p0
        ON DATEADD(
            'hour',
            23,
            block_date
        ) = p0.hour
        AND p0.token_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
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
        AND COALESCE(
            p0.price,
            p1.price
        ) IS NOT NULL
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_date,
        address,
        decimals,
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
    decimals,
    balance_hex,
    balance_raw,
    balance_precise,
    balance,
    balance_usd_heal AS balance_usd
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_date,
    address,
    decimals,
    balance_hex,
    balance_raw,
    balance_precise,
    balance,
    balance_usd,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','address']
    ) }} AS ez_balances_native_daily_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY ez_balances_native_daily_id
ORDER BY
    modified_timestamp DESC)) = 1
