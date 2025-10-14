{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    post_hook = '{{ unverify_stablecoins() }}',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STABLECOINS',
    } } },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

WITH mint_burn AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        event_name,
        token_address,
        s.symbol,
        s.name,
        s.decimals,
        from_address,
        to_address,
        amount_raw_precise,
        amount_raw,
        amount_precise,
        amount,
        IFF(
            s.decimals IS NOT NULL,
            ROUND(
                amount_precise * p.price,
                2
            ),
            NULL
        ) AS amount_usd,
        tx_succeeded,
        _log_id,
        s.modified_timestamp
    FROM
        {{ ref('silver__stablecoins_mint_burn') }}
        s
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND s.contract_address = p.token_address

{% if is_incremental() %}
AND s.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        event_name,
        token_address,
        t0.symbol,
        t0.name,
        t0.decimals,
        from_address,
        to_address,
        amount_raw_precise,
        amount_raw,
        amount_precise,
        amount,
        IFF(
            t0.decimals IS NOT NULL,
            ROUND(
                amount_precise * p.price,
                2
            ),
            NULL
        ) AS amount_usd,
        tx_succeeded,
        _log_id,
        t0.modified_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND t0.contract_address = p.token_address
    WHERE
        t0.block_number IN (
            SELECT
                t1.block_number
            FROM
                {{ this }}
                t1
            WHERE
                t1.amount_usd IS NULL
                AND t1.modified_timestamp < (
                    SELECT
                        MAX(
                            modified_timestamp
                        ) - INTERVAL '{{ vars.CURATED_COMPLETE_LOOKBACK_HOURS }}'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('price__ez_prices_hourly') }}
                        p
                    WHERE
                        p.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND p.price IS NOT NULL
                        AND p.token_address = t1.contract_address
                        AND p.hour = DATE_TRUNC(
                            'hour',
                            t1.block_timestamp
                        )
                )
            GROUP BY
                1
        )
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        mint_burn

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    *
FROM
    heal_model
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    token_address,
    symbol,
    NAME,
    decimals,
    from_address,
    to_address,
    amount_raw_precise,
    amount_raw,
    amount_precise,
    amount,
    amount_usd,
    tx_succeeded,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS ez_stablecoins_mint_burn_id
FROM
    FINAL
