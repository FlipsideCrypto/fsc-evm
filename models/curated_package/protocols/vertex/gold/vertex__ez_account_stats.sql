{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 
        'database_tags':{ 
            'table':{ 
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, STATS' 
            }
        }
    }
) }}

SELECT
    subaccount,
    trader,
    first_trade_timestamp,
    last_trade_timestamp,
    account_age,
    trade_count,
    DENSE_RANK() over (
        ORDER BY
            trade_count DESC
    ) AS trade_count_rank,
    trade_count_24h,
    DENSE_RANK() over (
        ORDER BY
            trade_count_24h DESC
    ) AS trade_count_rank_24h,
    perp_trade_count,
    spot_trade_count,
    long_count,
    short_count,
    total_usd_volume,
    DENSE_RANK() over (
        ORDER BY
            total_usd_volume DESC
    ) AS total_usd_volume_rank,
    total_usd_volume_24h,
    DENSE_RANK() over (
        ORDER BY
            total_usd_volume_24h DESC
    ) AS total_usd_volume_rank_24h,
    avg_usd_trade_size,
    total_fee_amount,
    total_base_delta_amount,
    total_quote_delta_amount,
    total_liquidation_amount,
    total_liquidation_amount_quote,
    total_liquidation_count,
    vertex_account_id as ez_account_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_account_stats') }}
ORDER BY total_usd_volume_rank DESC
